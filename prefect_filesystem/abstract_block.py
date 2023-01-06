"""
Abstract Block
"""

import os
from io import TextIOWrapper
from typing import IO, AnyStr, Tuple, Union

from anyio import AsyncFile
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem as FsSpecLocalFileSystem
from prefect.filesystems import LocalFileSystem as PrefectLocalFileSystem
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from prefect_filesystem.compression import compr


class BlockType:
    """
    Type definition
    """

    basepath: str
    filesystem: any


class AbstractBlock(BlockType):
    """
    Base methods for the Abstract Block
    """

    def _resolve_abstract_filesystem(self) -> AbstractFileSystem:
        """
        Resolves the provided filesystem into an AbstractFileSystem
        :return: AbstractFileSystem
        """
        """
        Resolves the provided filesystem into an AbstractFileSystem
        :return: AbstractFileSystem
        """
        return _resolve_abstract_filesystem(self.filesystem)

    def build_path(self, path: str) -> str:
        """
        Joins path to the basepath trimming any trailing '/'
        :param path:
        :return:
        """
        return os.path.join(self.basepath, path.lstrip("/"))

    def open(
        self, filepath: str, mode: str = "rb", compression=None, **kwargs
    ) -> IO[AnyStr]:
        """
        Opens the provided filename and applies compression wrappers. We apply
        custom compression wrappers due to the absense of some features in the
        fsspec compression wrappers.

        :param filepath:
        :param mode:
        :param compression:
        :param kwargs:
        :return: io.IOBase
        """

        fs = self._resolve_abstract_filesystem()
        full_path = self.build_path(filepath)

        compression, compression_options = _resolve_compression(compression)
        compress_fn = compr.get(compression)

        if compress_fn is None:
            return fs.open(full_path, mode, compression=compression, **kwargs)
        return _fs_open(fs, full_path, mode, compress_fn, compression_options, **kwargs)

    async def open_async(self, filename: str, mode: str = "rb", **kwargs) -> AsyncFile:
        """
        Async open file
        :param filename:
        :param mode:
        :param kwargs:
        :return:
        """
        return AsyncFile(
            await run_sync_in_worker_thread(self.open, filename, mode, **kwargs)
        )


def _fs_open(
    fs,
    path,
    mode,
    compress_fn,
    compression_options,
    encoding=None,
    errors=None,
    newline=None,
    **kwargs,
):
    """
    Helper function
    :param fs:
    :param path:
    :param mode:
    :param compress_fn:
    :param compression_options:
    :param encoding:
    :param errors:
    :param newline:
    :param kwargs:
    :return:
    """
    f = compress_fn(
        fs.open(path, mode.replace("t", "b"), **kwargs),
        mode,
        **(compression_options or {}),
    )

    return (
        f
        if "t" not in mode
        else TextIOWrapper(f, encoding=encoding, errors=errors, newline=newline)
    )


def _resolve_compression(compression) -> Tuple[str, Union[dict, None]]:
    """
    When compression is supplied as a dictionary we extract the compression_type and
    remove the "type" attribute from the dictionary
    :param compression:
    :return:
    """
    if not isinstance(compression, dict):
        return compression, None
    return (
        compression.get("type"),
        {k: v for k, v in compression.items() if k != "type"},
    )


def _resolve_abstract_filesystem(fs) -> AbstractFileSystem:
    """
    Resolves the provided filesystem into an AbstractFileSystem
    :return: AbstractFileSystem
    """
    """
    Resolves the provided filesystem into an AbstractFileSystem
    :return: AbstractFileSystem
    """
    if isinstance(fs, PrefectLocalFileSystem):
        return FsSpecLocalFileSystem()

    while not isinstance(fs, AbstractFileSystem) and hasattr(fs, "filesystem"):
        fs = fs.filesystem

    if not isinstance(fs, AbstractFileSystem):
        raise Exception("Expected an AbstractFileSystem")

    return fs
