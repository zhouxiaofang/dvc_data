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
from multiprocessing import Process
from .istextfile import DEFAULT_CHUNK_SIZE, istextblock
from .hash_info import HashInfo
from dvc_objects.fs.callbacks import DEFAULT_CALLBACK, Callback, TqdmCallback
import io
from typing import BinaryIO

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


def _build_file(path, fs, name, odb=None, upload_odb=None, dry_run=False):
    state = odb.state if odb else None
    meta, hash_info = hash_file(path, fs, name, state=state)
    if upload_odb and not dry_run:
        assert odb and name == "md5"
        return _upload_file(path, fs, odb, upload_odb)

    oid = hash_info.value
    if dry_run:
        obj = HashFile(path, fs, hash_info)
    else:
        odb.add(path, fs, oid, hardlink=False) #dvc数据库操作，log by zhoufang 20230111
        obj = odb.get(oid)

    return meta, obj

# start create by zhoufang, date 20230111
class LargeFileHashingCallback(TqdmCallback):
    """Callback that only shows progress bar if self.size > LARGE_FILE_SIZE."""

    LARGE_FILE_SIZE = 2**30

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("bytes", True)
        super().__init__(*args, **kwargs)
        self._logged = False
        self.fname = kwargs.get("desc", "")

    # TqdmCallback force renders progress bar on `set_size`.
    set_size = Callback.set_size

    def call(self, hook_name=None, **kwargs):
        if self.size and self.size > self.LARGE_FILE_SIZE:
            if not self._logged:
                logger.info(
                    f"Computing md5 for a large file '{self.fname}'. "
                    "This is only done once."
                )
                self._logged = True
            super().call()

def get_hasher(name: str) -> "hashlib._Hash":
    if name == "blake3":
        from blake3 import blake3

        return blake3(max_threads=blake3.AUTO)

    try:
        return getattr(hashlib, name)()
    except AttributeError:
        return hashlib.new(name)

def dos2unix(data: bytes) -> bytes:
    return data.replace(b"\r\n", b"\n")
         
class HashStreamFile(io.IOBase):
    def __init__(
        self,
        fobj: BinaryIO,
        hash_name: str = "md5",
        text: Optional[bool] = None,
    ) -> None:
        self.fobj = fobj
        self.total_read = 0
        self.hasher = get_hasher(hash_name)
        self.is_text: Optional[bool] = text
        super().__init__()

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.fobj.tell()

    def read(self, n=-1) -> bytes:
        chunk = self.fobj.read(n)
        if self.is_text is None and chunk:
            # do we need to buffer till the DEFAULT_CHUNK_SIZE?
            self.is_text = istextblock(chunk[:DEFAULT_CHUNK_SIZE])

        data = dos2unix(chunk) if self.is_text else chunk
        self.hasher.update(data)
        self.total_read += len(data)
        return chunk

    @property
    def hash_value(self) -> str:
        return self.hasher.hexdigest()

    @property
    def hash_name(self) -> str:
        return self.hasher.name
              
def hash_file_speed(
    path: "AnyFSPath",
    fs: "FileSystem",
    name: str,
    state: "StateBase" = None,
    callback: "Callback" = None,
) -> Tuple["Meta", "HashInfo"]:
    if state:
        meta, hash_info = state.get(path, fs)
        if hash_info:
            return meta, hash_info

    cb = callback or LargeFileHashingCallback(desc=path)
    with cb:
        meta = Meta.from_info(fs.info(path), fs.protocol)

        value = getattr(meta, name, None)
        if value:
            assert not value.endswith(".dir")
            return value, meta

        if hasattr(fs, name):
            func = getattr(fs, name)
            return str(func(path)), meta

        if name == "md5":
            size = fs.size(path) or 0
            cb.set_size(size)
            with fs.open(path, "rb") as fobj:
                chunk_size = 2**20
                assert chunk_size >= DEFAULT_CHUNK_SIZE
                stream = HashStreamFile(fobj, hash_name=name, text=None)
                while True:
                    data = stream.read(chunk_size)
                    if not data:
                        break
                hash_value = stream.hash_value
                hash_info = HashInfo(name, hash_value)
                if state:
                    assert ".dir" not in hash_info.value
                    state.save(path, fs, hash_info)
                return meta, hash_info

def _build_file_speed(path, fs, name, odb=None, upload_odb=None, dry_run=False):
    state = odb.state if odb else None
    meta, hash_info = hash_file_speed(path, fs, name, state=state)
    if upload_odb and not dry_run:
        assert odb and name == "md5"
        return _upload_file(path, fs, odb, upload_odb)

    oid = hash_info.value
    if dry_run:
        obj = HashFile(path, fs, hash_info)
    else:
        odb.add(path, fs, oid, hardlink=False) #dvc数据库操作，log by zhoufang 20230111
        obj = odb.get(oid)

    return meta, obj
# end create by zhoufang, date 20230111



def slice_fnames(process_jobs, fnames_list ):  
    print("build()开启的进程数目为：", process_jobs)
    fnames_list_length = len(fnames_list)
    n = process_jobs
    
    fnames_slice_list = []
    for i in range(n):
        one_thread_list = fnames_list[math.floor(i / n * fnames_list_length): math.floor((i + 1) / n * fnames_list_length)]
        one_thread_set = set(one_thread_list)
        fnames_slice_list.append(one_thread_set)
    print("主进程成功获取所有文件名字，fnames_slice_list文件对象的切片数目：", len(fnames_slice_list))
    
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
    # t1 = datetime.now()
    with Tqdm(
        unit="obj",
        desc=f"New debug func: Building data objects from {relpath}",
        disable=no_progress_bar,
    ) as pbar:
        for root, _, fnames in walk_iter:
            if DefaultIgnoreFile in fnames:
                raise IgnoreInCollectedDirError(
                    DefaultIgnoreFile, fs.path.join(root, DefaultIgnoreFile)
                )

            # NOTE: we know for sure that root starts with path, so we can use
            # faster string manipulation instead of a more robust relparts()
            rel_key: Tuple[Optional[Any], ...] = ()
            if root != path:
                rel_key = tuple(root[len(path) + 1 :].split(fs.sep))

            for fname in fnames: # add by zhoufang in 20230106,for循环可以并行加速遍历fnames
                if fname == "":
                    # NOTE: might happen with s3/gs/azure/etc, where empty
                    # objects like `dir/` might be used to create an empty dir
                    continue

                pbar.update()
                meta, obj = _build_file(
                    f"{root}{fs.sep}{fname}", fs, name, odb=odb, **kwargs
                )
                key = (*rel_key, fname)
                tree.add(key, meta, obj.hash_info)
                tree_meta.size += meta.size or 0
                tree_meta.nfiles += 1
            
            # 改进比对操作为目标函数，start create by zhoufang at 20221226  
            # def target_func(fnames_list):
            #     for fname in fnames_list: # add by zhoufang in 20230106,for循环可以并行加速遍历fnames
            #         if fname == "":
            #             # NOTE: might happen with s3/gs/azure/etc, where empty
            #             # objects like `dir/` might be used to create an empty dir
            #             continue

            #         pbar.update()
            #         meta, obj = _build_file(
            #             f"{root}{fs.sep}{fname}", fs, name, odb=odb, **kwargs
            #         )
            #         key = (*rel_key, fname)
            #         tree.add(key, meta, obj.hash_info)
            #         tree_meta.size += meta.size or 0
            #         tree_meta.nfiles += 1
            
            # fnames_slice_list = slice_fnames(6, fnames) 
            # time1 = datetime.now()
            # print("build并行加速遍历fnames操作, starting ...")
            # process_list = []
            # for i in range(6):
            #     t = Process(target=target_func, args=(fnames_slice_list[i], ))
            #     t.start()
            #     process_list.append(t) 
            # for t in process_list:
            #     t.join()
        
            # time2 = datetime.now()
            # print("build并行进程池操作, 主进程执行的总时间：", time2 - time1)
    # t2 = datetime.now()
    # print("success call in _build_tree, the core is foreach _build_file(),cost_time:", (t2 - t1))
    
    tree.digest()
    tree = add_update_tree(odb, tree)
    return tree_meta, tree


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
