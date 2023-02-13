import logging
from itertools import chain

from dvc_objects.fs.callbacks import Callback
from dvc_objects.fs.generic import test_links, transfer

from .build import build
from .diff import ROOT
from .diff import diff as odiff

from datetime import datetime
from shutil import copyfile
from attrs import asdict, define, field
import reprlib
from typing import TYPE_CHECKING, List, Optional, Tuple, Dict

if TYPE_CHECKING:
    from .hashfile._ignore import Ignore


from dvc_objects.executors_process import ProcessPoolExecutor
from multiprocessing import Process
import math

logger = logging.getLogger(__name__)

ADD = "add"
MODIFY = "modify"
DELETE = "delete"
UNCHANGED = "unchanged"


class PromptError(Exception):
    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"unable to remove '{path}' without a confirmation.")


class CheckoutError(Exception):
    def __init__(self, paths: List[str]) -> None:
        self.paths = paths
        super().__init__("Checkout failed")


class LinkError(Exception):
    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__("No possible cache link types for '{path}'.")


def _remove(path, fs, in_cache, force=False, prompt=None):
    if not fs.exists(path):
        return

    if force:
        fs.remove(path)
        return

    if not in_cache:
        msg = (
            f"file/directory '{path}' is going to be removed. "
            "Are you sure you want to proceed?"
        )

        if prompt is None or not prompt(msg):
            raise PromptError(path)

    fs.remove(path)


def _relink(link, cache, cache_info, fs, path, in_cache, force, prompt=None):
    _remove(path, fs, in_cache, force=force, prompt=prompt)
    link(cache, cache_info, fs, path)
    # NOTE: Depending on a file system (e.g. on NTFS), `_remove` might reset
    # read-only permissions in order to delete a hardlink to protected object,
    # which will also reset it for the object itself, making it unprotected,
    # so we need to protect it back.
    cache.protect(cache_info)


def _checkout_file(
    link,
    path,
    fs,
    change,
    cache,
    force,
    relink=False,
    state=None,
    prompt=None,
):
    """The file is changed we need to checkout a new copy"""
    modified = False

    cache_path = cache.oid_to_path(change.new.oid.value)
    if change.old.oid:
        if relink:
            if fs.iscopy(path) and cache.cache_types[0] == "copy":
                cache.unprotect(path)
            else:
                _relink(
                    link,
                    cache,
                    cache_path,
                    fs,
                    path,
                    change.old.in_cache,
                    force=force,
                    prompt=prompt,
                )
        else:
            modified = True
            _relink(
                link,
                cache,
                cache_path,
                fs,
                path,
                change.old.in_cache,
                force=force,
                prompt=prompt,
            )
    else:
        link(cache, cache_path, fs, path)
        modified = True

    if state:
        state.save(path, fs, change.new.oid)

    return modified

#zhoufang add funcs start
@define(hash=True, order=True)
class TreeEntry:
    in_cache: bool = field(default=False, eq=False)
    key: Tuple[str, ...] = ()
    meta: Optional["Meta"] = field(default=None, eq=False)
    oid: Optional["HashInfo"] = None

    def __bool__(self):
        return bool(self.oid)

@define(hash=True, order=True)
class Change:
    old: TreeEntry = field(factory=TreeEntry)
    new: TreeEntry = field(factory=TreeEntry)
    typ: str = field(init=False)

    @typ.default
    def _(self):
        if not self.old and not self.new:
            return UNCHANGED

        if self.old and not self.new:
            return DELETE

        if not self.old and self.new:
            return ADD

        if self.old != self.new:
            return MODIFY

        return UNCHANGED

    def __bool__(self):
        return self.typ != UNCHANGED
    
@define
class DiffResult:
    added: List[Change] = field(factory=list, repr=reprlib.repr)
    modified: List[Change] = field(factory=list, repr=reprlib.repr)
    deleted: List[Change] = field(factory=list, repr=reprlib.repr)
    unchanged: List[Change] = field(factory=list, repr=reprlib.repr)

    def __bool__(self):
        return bool(self.added or self.modified or self.deleted)

    @property
    def stats(self) -> Dict[str, int]:
        return {k: len(v) for k, v in asdict(self).items()}
#zhoufang add funcs end
 
# create new func by zhoufang, date 20221223
def slice_keys(process_jobs, old_keys, new_keys ):  
    src_old_keys = list(old_keys)
    src_new_keys = list(new_keys)
    print("开启的进程数目为：", process_jobs)
    old_length = len(src_old_keys)
    new_length = len(src_new_keys)
    n = process_jobs
    
    old_keys_list = []
    for i in range(n):
        one_thread_list = src_old_keys[math.floor(i / n * old_length): math.floor((i + 1) / n * old_length)]
        one_thread_set = set(one_thread_list)
        old_keys_list.append(one_thread_set)
    print("主进程成功获取所有文件图片，old_keys_list写文件的切片数目：", len(old_keys_list))
    
    new_keys_list = []
    for j in range(n):
        one_slice_list = src_new_keys[math.floor(j / n * new_length): math.floor((j + 1) / n * new_length)]
        one_slice_set = set(one_slice_list)
        new_keys_list.append(one_slice_set)
    print("主进程成功获取所有文件图片，new_keys_list写文件的切片数目：", len(new_keys_list))
    
    return old_keys_list, new_keys_list

# create new func by zhoufang, date 20221223
def speed_diff(
    path,
    fs,
    new,
    cache,
    state,
    prompt,
    progress_callback,
    force,
    relink=False,
    quiet=False,
    ignore: Optional["Ignore"] = None,
):
    print("success starting to use speed resolution, call speed_diff()")
    t_0 = datetime.now()
    old = None
    try:
        _, _, old = build(
            cache,
            path,
            fs,
            new.hash_info.name if new else cache.hash_name,
            dry_run=True,
            ignore=ignore,
        )
    except FileNotFoundError:
        pass
    t_1 = datetime.now()
    time_diff = t_1 - t_0
    print("success call in speed_diff() step1.1: [工作区和缓存区 元数据获取操作, _build()] #{}".format(time_diff))


    # diff = odiff(old, obj, cache) # dvc源码
    from .tree import Tree

    if old is None and new is None:
        return DiffResult()

    def _get_keys(obj):
        if not obj:
            return []
        return [ROOT] + (
            [key for key, _, _ in obj] if isinstance(obj, Tree) else []
        )

    old_keys = set(_get_keys(old))
    new_keys = set(_get_keys(new))
    

    def _get(obj, key):
        if not obj or key == ROOT:
            return None, (obj.hash_info if obj else None)
        if not isinstance(obj, Tree):
            # obj is not a Tree and key is not a ROOT
            # hence object does not exist for a given key
            return None, None
        return obj.get(key, (None, None))

    def _in_cache(oid, cache):
        from dvc_objects.errors import ObjectFormatError

        if not oid:
            return False

        try:
            cache.check(oid.value)
            return True
        except (FileNotFoundError, ObjectFormatError):
            return False
    
    # 改进比对操作为目标函数，start create by zhoufang at 20221226
    def target_func(old_keys_slice, new_keys_slice):
        local_process_ret = DiffResult()
        for key in old_keys_slice | new_keys_slice:
            old_meta, old_oid = _get(old, key)
            new_meta, new_oid = _get(new, key)

            change = Change(
                old=TreeEntry(_in_cache(old_oid, cache), key, old_meta, old_oid),
                new=TreeEntry(_in_cache(new_oid, cache), key, new_meta, new_oid),
            )

            if change.typ == ADD:
                local_process_ret.added.append(change)
            elif change.typ == MODIFY:
                local_process_ret.modified.append(change)
            elif change.typ == DELETE:
                local_process_ret.deleted.append(change)
            else:
                assert change.typ == UNCHANGED
                local_process_ret.unchanged.append(change)
        
        if relink: 
            local_process_ret.modified.extend(local_process_ret.unchanged)
        else:
            for change in local_process_ret.unchanged:  # pylint: disable=not-an-iterable
                if not change.new.in_cache and not (
                    change.new.oid and change.new.oid.isdir
                ):
                    local_process_ret.modified.append(change)
                    
        failed = []
        if not new:
            if not quiet:
                logger.warning(
                    "No file hash info found for '%s'. It won't be created.",
                    path,
                )
            failed.append(path)
            
        try:
            _checkout(
                local_process_ret,
                path,
                fs,
                cache,
                force,
                relink=relink,
                state=state,
                prompt=prompt,
            )
            
        except CheckoutError as exc:
            failed.extend(exc.paths)

        if failed or not local_process_ret:
            if progress_callback and new:
                progress_callback(path, len(new))
            if failed:
                raise CheckoutError(failed)
            return
    # 改进比对操作为目标函数，end create by zhoufang at 20221226
    
    old_keys_slice_list, new_keys_slice_list = slice_keys(6, old_keys, new_keys) 
          
    print("odiff并行加速对比操作, starting ...")
    process_list = []
    for i in range(6):
        t = Process(target=target_func, args=(old_keys_slice_list[i], new_keys_slice_list[i], ))
        t.start()
        process_list.append(t) 
    for t in process_list:
        t.join()
        

def _diff( #dvc原始代码
    path,
    fs,
    obj,
    cache,
    relink=False,
    ignore: Optional["Ignore"] = None,
):
    
    old = None
    try:
        _, _, old = build(
            cache,
            path,
            fs,
            obj.hash_info.name if obj else cache.hash_name,
            dry_run=True,
            ignore=ignore,
        )
    except FileNotFoundError:
        pass
    
    diff = odiff(old, obj, cache)
    
    if relink:
        diff.modified.extend(diff.unchanged)
    else:
        for change in diff.unchanged:  # pylint: disable=not-an-iterable
            if not change.new.in_cache and not (
                change.new.oid and change.new.oid.isdir
            ):
                diff.modified.append(change)

    return diff


class Link:
    def __init__(self, links):
        self._links = links

    def __call__(self, cache, from_path, to_fs, to_path, callback=None):
        if to_fs.exists(to_path):
            to_fs.remove(to_path)  # broken symlink

        parent = to_fs.path.parent(to_path)
        to_fs.makedirs(parent)
        try:
            with Callback.as_tqdm_callback(
                callback,
                desc=cache.fs.path.name(from_path),
                bytes=True,
            ) as cb:
                transfer( 
                    cache.fs,
                    from_path,
                    to_fs,
                    to_path,
                    links=self._links,
                    callback=cb,
                )
            
        except FileNotFoundError as exc:
            raise CheckoutError([to_path]) from exc
        except OSError as exc:
            raise LinkError(to_path) from exc 

def _checkout(
    diff,
    path,
    fs,
    cache,
    force=False,
    progress_callback=None,
    relink=False,
    state=None,
    prompt=None,
):
    if not diff:
        return

    links = test_links(cache.cache_types, cache.fs, cache.path, fs, path)
    if not links:
        raise LinkError(path)
    link = Link(links)
    for change in diff.deleted:
        entry_path = (
            fs.path.join(path, *change.old.key)
            if change.old.key != ROOT
            else path
        )
        _remove(
            entry_path, fs, change.old.in_cache, force=force, prompt=prompt
        )

    failed = []
    for change in chain(diff.added, diff.modified):
        entry_path = (
            fs.path.join(path, *change.new.key)
            if change.new.key != ROOT
            else path
        )
        if change.new.oid.isdir:
            fs.makedirs(entry_path)
            continue

        try:
            _checkout_file(
                link,
                entry_path,
                fs,
                change,
                cache,
                force,
                relink,
                state=state,
                prompt=prompt,
            )
            if progress_callback:
                progress_callback(entry_path)
        except CheckoutError as exc:
            failed.extend(exc.paths)

    if failed:
        raise CheckoutError(failed)
    
def checkout(
    path,
    fs,
    obj,
    cache,
    force=False,
    progress_callback=None,
    relink=False,
    quiet=False,
    ignore: Optional["Ignore"] = None,
    state=None,
    prompt=None,
):
    # if protocol(path) not in ["local", cache.fs.protocol]:
    #    raise NotImplementedError
    
    # t1 = datetime.now()
    # diff = _diff( #dvc源码，调用_diff比对函数
    #     path,
    #     fs,
    #     obj,
    #     cache,
    #     relink=relink,
    #     ignore=ignore,
    # ) #dvc源码
    speed_diff( #dvc版本切换场景，最新优化版本的比对函数
        path,
        fs,
        obj,
        cache,
        state,
        prompt,
        progress_callback,
        force,
        relink=relink,
        quiet=False,
        ignore=ignore,
    )
    if state:
        state.save_link(path, fs)
    
    # dvc源码部分start
    # failed = [] 
    # if not obj:
    #     if not quiet:
    #         logger.warning(
    #             "No file hash info found for '%s'. It won't be created.",
    #             path,
    #         )
    #     failed.append(path)
    # try:
    #     _checkout(
    #         diff,
    #         path,
    #         fs,
    #         cache,
    #         force=force,
    #         progress_callback=progress_callback,
    #         relink=relink,
    #         state=state,
    #         prompt=prompt,
    #     )
    # except CheckoutError as exc:
    #     failed.extend(exc.paths)

    # if diff and state:
    #     state.save_link(path, fs)

    # if failed or not diff:
    #     if progress_callback and obj:
    #         progress_callback(path, len(obj))
    #     if failed:
    #         raise CheckoutError(failed)
    #     return
    # return bool(diff) and not relink
    # dvc源码部分 end
    return not relink #speed_diff



# dvc源码 _checkout,备份注释20221226
# def _checkout(
#     diff,
#     path,
#     fs,
#     cache,
#     force=False,
#     progress_callback=None,
#     relink=False,
#     state=None,
#     prompt=None,
# ):
#     if not diff:
#         return

#     links = test_links(cache.cache_types, cache.fs, cache.path, fs, path)
#     if not links:
#         raise LinkError(path)
#     link = Link(links)
#     for change in diff.deleted:
#         entry_path = (
#             fs.path.join(path, *change.old.key)
#             if change.old.key != ROOT
#             else path
#         )
#         _remove(
#             entry_path, fs, change.old.in_cache, force=force, prompt=prompt
#         )

#     failed = []
#     for change in chain(diff.added, diff.modified):
#         entry_path = (
#             fs.path.join(path, *change.new.key)
#             if change.new.key != ROOT
#             else path
#         )
#         if change.new.oid.isdir:
#             fs.makedirs(entry_path)
#             continue

#         try:
#             _checkout_file(
#                 link,
#                 entry_path,
#                 fs,
#                 change,
#                 cache,
#                 force,
#                 relink,
#                 state=state,
#                 prompt=prompt,
#             )
#             if progress_callback:
#                 progress_callback(entry_path)
#         except CheckoutError as exc:
#             failed.extend(exc.paths)

#     if failed:
#         raise CheckoutError(failed) 

# dvc源码checkout,备份注释20221226
# def checkout(
#     path,
#     fs,
#     obj,
#     cache,
#     force=False,
#     progress_callback=None,
#     relink=False,
#     quiet=False,
#     ignore: Optional["Ignore"] = None,
#     state=None,
#     prompt=None,
# ):
#     # if protocol(path) not in ["local", cache.fs.protocol]:
#     #    raise NotImplementedError
#     diff = _diff(
#         path,
#         fs,
#         obj,
#         cache,
#         relink=relink,
#         ignore=ignore,
#     )
    
#     failed = []
#     if not obj:
#         if not quiet:
#             logger.warning(
#                 "No file hash info found for '%s'. It won't be created.",
#                 path,
#             )
#         failed.append(path)

#     try:
#         _checkout(
#             diff,
#             path,
#             fs,
#             cache,
#             force=force,
#             progress_callback=progress_callback,
#             relink=relink,
#             state=state,
#             prompt=prompt,
#         )
#     except CheckoutError as exc:
#         failed.extend(exc.paths)

#     if diff and state:
#         state.save_link(path, fs)

#     if failed or not diff:
#         if progress_callback and obj:
#             progress_callback(path, len(obj))
#         if failed:
#             raise CheckoutError(failed)
#         return
    
#     return bool(diff) and not relink