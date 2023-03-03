import hashlib
import logging
import os
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple


from dvc_objects._tqdm import Tqdm

from .db.reference import ReferenceHashFileDB
from .hash import hash_file
from .meta import Meta
from .obj import HashFile

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem

    from ._ignore import Ignore
    from .db import HashFileDB

from datetime import datetime
import math
from multiprocessing import Process, Manager, Pool
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from .istextfile import DEFAULT_CHUNK_SIZE, istextblock
from .hash_info import HashInfo
from dvc_objects.fs.callbacks import DEFAULT_CALLBACK, Callback, TqdmCallback
import io
from typing import BinaryIO
import time
from .zfang_gol import zfang_gol
import threading

DefaultIgnoreFile = ".dvcignore"


class IgnoreInCollectedDirError(Exception):
    def __init__(self, ignore_file: str, ignore_dirname: str) -> None:
        super().__init__(
            f"{ignore_file} file should not be in collected dir path: "
            f"'{ignore_dirname}'"
        )


logger = logging.getLogger(__name__)


_STAGING_MEMFS_PATH = "dvc-staging"


def _upload_file(from_path, fs, odb, upload_odb, callback=None):
    from dvc_objects.fs.callbacks import Callback
    from dvc_objects.fs.utils import tmp_fname

    from .hash import HashStreamFile

    path = upload_odb.fs.path
    tmp_info = path.join(upload_odb.path, tmp_fname())
    with fs.open(from_path, mode="rb") as stream:
        stream = HashStreamFile(stream)
        size = fs.size(from_path)
        with Callback.as_tqdm_callback(
            callback,
            desc=path.name(from_path),
            bytes=True,
            size=size,
        ) as cb:
            upload_odb.fs.put_file(stream, tmp_info, size=size, callback=cb)

    oid = stream.hash_value
    odb.add(tmp_info, upload_odb.fs, oid)
    meta = Meta(size=size)
    return meta, odb.get(oid)

t_build_file1_total = 0.0
t_build_file2_total = 0.0
zfang_gol._init()
zfang_gol.set_value("db_add_time", 0.0)
zfang_gol.set_value("hash_file_time", 0.0)
zfang_gol.set_value("other_db_time", 0.0)
zfang_gol.set_value("hash_store", 0.0)
zfang_gol.set_value("other_hash_store", 0.0)
zfang_gol.set_value("sqlite_store", 0.0)
def _build_file(path, fs, name, odb=None, upload_odb=None, dry_run=False):
    state = odb.state if odb else None
    meta, hash_info = hash_file(path, fs, name, state=state)
    
    if upload_odb and not dry_run:
        assert odb and name == "md5"
        return _upload_file(path, fs, odb, upload_odb)
    # hash_file_s1 = time.time()
    # oid = hash_info.value
    # if dry_run:
    #     obj = HashFile(path, fs, hash_info)
    # else:
    #     odb.add(path, fs, oid, hardlink=False)
    #     obj = odb.get(oid)
    # hash_file_e1 = time.time()
    # zfang_gol.set_value("other_db_time", zfang_gol.get_value("other_db_time") + (hash_file_e1 - hash_file_s1))
    
    # return meta, obj
    return meta, hash_info #speed verion create by zhoufang in 20230117

def slice_fnames(process_jobs, fnames_list ):  # add by zhoufang in 20230211
    fnames_list_length = len(fnames_list)
    n = process_jobs
    
    fnames_slice_list = []
    for i in range(n):
        one_thread_list = fnames_list[math.floor(i / n * fnames_list_length): math.floor((i + 1) / n * fnames_list_length)]
        one_thread_set = set(one_thread_list)
        fnames_slice_list.append(one_thread_set)
    
    return fnames_slice_list


def _build_tree(
    path,
    fs,
    fs_info,
    name,
    odb=None,
    ignore: "Ignore" = None,
    no_progress_bar=False,
    **kwargs,
):
    from .db import add_update_tree
    from .hash_info import HashInfo
    from .tree import Tree
    
    value = fs_info.get(name)
    if odb and value:
        try:
            tree = Tree.load(odb, HashInfo(name, value))
            return Meta(nfiles=len(tree)), tree
        except FileNotFoundError:
            pass

    if ignore:
        walk_iter = ignore.walk(fs, path)
    else:
        walk_iter = fs.walk(path)

    tree_meta = Meta(size=0, nfiles=0, isdir=True)
    # assuring mypy that they are not None but integer
    assert tree_meta.size is not None
    assert tree_meta.nfiles is not None

    tree = Tree()

    try:
        relpath = fs.path.relpath(path)
    except ValueError:
        # NOTE: relpath might not exist
        relpath = path
    
    t_build = 0.0
    with Tqdm(
        unit="obj",
        desc=f"Building data objects from {relpath}",
        disable=no_progress_bar,
    ) as pbar:
        for root, _, fnames in walk_iter:
            new_add_fnames = []
            if DefaultIgnoreFile in fnames:
                raise IgnoreInCollectedDirError(
                    DefaultIgnoreFile, fs.path.join(root, DefaultIgnoreFile)
                )

            # NOTE: we know for sure that root starts with path, so we can use
            # faster string manipulation instead of a more robust relparts()
            rel_key: Tuple[Optional[Any], ...] = ()
            if root != path:
                rel_key = tuple(root[len(path) + 1 :].split(fs.sep))
                  
            # create 20230207
            if len(fnames) != 0: 
                t1 = time.time()
                state = odb.state if odb else None
                for fname in fnames:
                    temp_path = f"{root}{fs.sep}{fname}"
                    meta, db_hash_info = state.get(temp_path, fs)
                    if db_hash_info == None:
                        new_add_fnames.append(fname)
                t2 = time.time()
                t_build += (t2 - t1)

                global_info_list = []
                def target_func(fnames_list):
                    for fname in fnames_list: # add by zhoufang in 20230106
                        if fname == "":
                            # NOTE: might happen with s3/gs/azure/etc, where empty
                            # objects like `dir/` might be used to create an empty dir
                            continue
                        pbar.update()
                        file_path = f"{root}{fs.sep}{fname}"
                        meta, hash_info = _build_file(
                            f"{root}{fs.sep}{fname}", fs, name, odb=odb, **kwargs
                        )      
                        temTuple = (fname, meta, file_path, hash_info)
                        global_info_list.append(temTuple)
        
                if state:
                    if len(new_add_fnames) != 0:  
                        N = 2    
                        fnames_slice_list = slice_fnames(N, new_add_fnames)
                        threads = []
                        for i in range(N):
                            t = threading.Thread(target=target_func, args=(fnames_slice_list[i],))
                            threads.append(t) 
                        for t in threads:
                            t.start()
                        for t in threads:
                            t.join()
                        
                        t_sqlite = 0.0
                        if len(global_info_list) != 0:
                            for (fname, meta, path, hash_info) in global_info_list:                   
                                if state:
                                    assert ".dir" not in hash_info.value
                                    t3 = time.time() 
                                    state.save_batch(path, fs, hash_info) 
                                    t4 = time.time()  
                                    t_sqlite += (t4-t3)    
                            state.commit_batch()
                            print(" in Tree build, _build_tree() meta storage total time2={0}".format(t_sqlite))

                t5 = time.time()
                for fname in fnames:
                    path = f"{root}{fs.sep}{fname}"
                    if fname == "":
                        # NOTE: might happen with s3/gs/azure/etc, where empty
                        # objects like `dir/` might be used to create an empty dir
                        continue
        
                    pbar.update()
                    meta, hash_info = state.get(path, fs)
        
                    key = (*rel_key, fname)
                    tree.add(key, meta, hash_info)
                    tree_meta.size += meta.size or 0
                    tree_meta.nfiles += 1
                    odb.add(path, fs, hash_info.value, hardlink=False)
                t6 = time.time()
                t_build += (t6 - t5)     

    tree.digest()
    tree = add_update_tree(odb, tree)
    print(" in Tree build, _build_tree() meta build total time1=", t_build)
    print("\n-----------------------\n")
    return tree_meta, tree
                
# def _build_tree( #dvc源码
#     path,
#     fs,
#     fs_info,
#     name,
#     odb=None,
#     ignore: "Ignore" = None,
#     no_progress_bar=False,
#     **kwargs,
# ):
#     from .db import add_update_tree
#     from .hash_info import HashInfo
#     from .tree import Tree

#     value = fs_info.get(name)
#     if odb and value:
#         try:
#             tree = Tree.load(odb, HashInfo(name, value))
#             return Meta(nfiles=len(tree)), tree
#         except FileNotFoundError:
#             pass

#     if ignore:
#         walk_iter = ignore.walk(fs, path)
#     else:
#         walk_iter = fs.walk(path)

#     tree_meta = Meta(size=0, nfiles=0, isdir=True)
#     # assuring mypy that they are not None but integer
#     assert tree_meta.size is not None
#     assert tree_meta.nfiles is not None

#     tree = Tree()

#     try:
#         relpath = fs.path.relpath(path)
#     except ValueError:
#         # NOTE: relpath might not exist
#         relpath = path

#     with Tqdm(
#         unit="obj",
#         desc=f"Building data objects from {relpath}",
#         disable=no_progress_bar,
#     ) as pbar:
#         for root, _, fnames in walk_iter:
#             if DefaultIgnoreFile in fnames:
#                 raise IgnoreInCollectedDirError(
#                     DefaultIgnoreFile, fs.path.join(root, DefaultIgnoreFile)
#                 )

#             # NOTE: we know for sure that root starts with path, so we can use
#             # faster string manipulation instead of a more robust relparts()
#             rel_key: Tuple[Optional[Any], ...] = ()
#             if root != path:
#                 rel_key = tuple(root[len(path) + 1 :].split(fs.sep))

            # for fname in fnames:
            #     if fname == "":
            #         # NOTE: might happen with s3/gs/azure/etc, where empty
            #         # objects like `dir/` might be used to create an empty dir
            #         continue

            #     pbar.update()
            #     meta, obj = _build_file(
            #         f"{root}{fs.sep}{fname}", fs, name, odb=odb, **kwargs
            #     )
            #     key = (*rel_key, fname)
            #     tree.add(key, meta, obj.hash_info)
            #     tree_meta.size += meta.size or 0
            #     tree_meta.nfiles += 1

#     tree.digest()
#     tree = add_update_tree(odb, tree)
#     return tree_meta, tree


_url_cache: Dict[str, str] = {}


def _make_staging_url(
    fs: "FileSystem", odb: "HashFileDB", path: Optional[str]
):
    from dvc_objects.fs import Schemes

    url = f"{Schemes.MEMORY}://{_STAGING_MEMFS_PATH}"

    if path is not None:
        if odb.fs.protocol == Schemes.LOCAL:
            path = os.path.abspath(path)

        if path not in _url_cache:
            _url_cache[path] = hashlib.sha256(path.encode("utf-8")).hexdigest()

        url = fs.path.join(url, _url_cache[path])

    return url


def _get_staging(odb: "HashFileDB") -> "ReferenceHashFileDB":
    """Return an ODB that can be used for staging objects.

    Staging will be a reference ODB stored in the the global memfs.
    """

    from dvc_objects.fs import MemoryFileSystem

    fs = MemoryFileSystem()
    path = _make_staging_url(fs, odb, odb.path)
    state = odb.state
    return ReferenceHashFileDB(fs, path, state=state, hash_name=odb.hash_name)


def _build_external_tree_info(odb, tree, name):
    # NOTE: used only for external outputs. Initial reasoning was to be
    # able to validate .dir files right in the workspace (e.g. check s3
    # etag), but could be dropped for manual validation with regular md5,
    # that would be universal for all clouds.
    assert odb and name != "md5"

    oid = tree.hash_info.value
    odb.add(tree.path, tree.fs, oid)
    raw = odb.get(oid)
    _, hash_info = hash_file(raw.path, raw.fs, name, state=odb.state)
    tree.path = raw.path
    tree.fs = raw.fs
    tree.hash_info.name = hash_info.name
    tree.hash_info.value = hash_info.value
    if not tree.hash_info.value.endswith(".dir"):
        tree.hash_info.value += ".dir"
    return tree


def build(
    odb: "HashFileDB",
    path: "AnyFSPath",
    fs: "FileSystem",
    name: str,
    upload: bool = False,
    dry_run: bool = False,
    **kwargs,
) -> Tuple["HashFileDB", "Meta", "HashFile"]:
    """Stage (prepare) objects from the given path for addition to an ODB.

    Returns at tuple of (object_store, object) where addition to the ODB can
    be completed by transferring the object from object_store to the dest ODB.

    If dry_run is True, object hashes will be computed and returned, but file
    objects themselves will not be added to the object_store ODB (i.e. the
    resulting file objects cannot transferred from object_store to another
    ODB).

    If upload is True, files will be uploaded to a temporary path on the dest
    ODB filesystem, and built objects will reference the uploaded path rather
    than the original source path.
    """
    assert path
    # assert protocol(path) == fs.protocol
    
    details = fs.info(path)
    staging = _get_staging(odb)

    if details["type"] == "directory":
        meta, obj = _build_tree(
            path,
            fs,
            details,
            name,
            odb=staging,
            upload_odb=odb if upload else None,
            dry_run=dry_run,
            **kwargs,
        )
        logger.debug("built tree '%s'", obj)
        if name != "md5":
            obj = _build_external_tree_info(odb, obj, name)
    else:
        meta, obj = _build_file(
            path,
            fs,
            name,
            odb=staging,
            upload_odb=odb if upload else None,
            dry_run=dry_run,
        )
    
    return staging, meta, obj