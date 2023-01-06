"""
Provides a collection of filesystem tasks
"""

import json
from typing import Any, Callable, Optional, Union

from prefect import get_run_logger, task
from prefect.blocks.core import Block
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from .abstract_local_filesystem import AbstractLocalFileSystem
from .utlity import CompressionType, apply_path_format, copy_filesystem, ensure_abstract


@task
async def filesystem_put(
    content: any,
    filename: str,
    filesystem: Block = None,
    compression: Union[str, CompressionType] = None,
    **kwargs,
) -> str:
    """
    A Prefect task to write the provided content into the provided file
    :param compression:
    :param content:
    :param filename:
    :param filesystem:
    :param kwargs:
    :return:
    """
    logger = get_run_logger()
    filesystem = ensure_abstract(filesystem)
    mode = "wb" if isinstance(content, bytes) else "wt"

    async with await filesystem.open_async(
        filename, mode=mode, compression=compression, **kwargs
    ) as output_fd:
        if isinstance(content, (list, dict, tuple)):
            await run_sync_in_worker_thread(json.dump, content, output_fd.wrapped)
        else:
            await output_fd.write(content)

    logger.info(f"Written to {filename}")

    return filesystem.build_path(filename)


NOT_PROVIDED = object()


@task
async def filesystem_get(
    filename: str,
    filesystem: Block,
    compression: Union[str, CompressionType] = None,
    encoding: Optional[str] = "utf-8",
    transform: Optional[Union["json", Callable[[Any], Any]]] = None,
    default_value: Optional[Any] = NOT_PROVIDED,
    **kwargs,
):
    """
    Prefect task to return the content of the supplied file. Optionally this
    will also transform the content.

    :param filename:
    :param filesystem:
    :param compression:
    :param encoding:
    :param transform:
    :param default_value:
    :param kwargs:
    :return:
    """
    logger = get_run_logger()
    filesystem = ensure_abstract(filesystem)

    mode = "rb" if encoding is None or encoding == "none" else "rt"

    try:
        async with await filesystem.open_async(
            filename, mode=mode, encoding=encoding, compression=compression, **kwargs
        ) as output_fd:
            content = await output_fd.read()
            logger.info(f"Read from {filename} {content}")
            return (
                json.loads(content)
                if transform == "json"
                else transform(content)
                if callable(transform)
                else content
            )
    except FileNotFoundError as ex:
        logger.info(f"File does not exist {filename}")
        if default_value is NOT_PROVIDED:
            raise ex
        return default_value


@task
async def filesystem_copy(
    source_filename: str,
    source_filesystem: Block,
    target_filesystem: str,
    target_filename: str = None,
    source_compression: Union[str, CompressionType] = None,
    target_compression: Union[str, CompressionType] = None,
    datasource: Optional[Any] = None,
    block_size=1024 * 1024,
) -> list:
    """
    Copies data from the source filesystem into the target filesystem
    :param source_filename:
    :param source_filesystem:
    :param target_filesystem:
    :param target_filename:
    :param source_compression:
    :param target_compression:
    :param datasource:
    :param block_size:
    :return:
    """
    logger = get_run_logger()

    source_filesystem = ensure_abstract(source_filesystem)
    target_filesystem = ensure_abstract(target_filesystem)

    target_filename = target_filename or source_filename

    with AbstractLocalFileSystem.make_temp(auto_mkdir=True) as stage_fs:
        resolved_names = [
            (
                apply_path_format(source_metadata, source_filename, source_compression),
                apply_path_format(source_metadata, target_filename, target_compression),
            )
            for source_metadata in _as_iterable(datasource)
        ]

        if len(resolved_names) == 0:
            logger.info("Nothing to do")
            return []

        for i, o in resolved_names:
            logger.info(f"Staging {i.path}")
            await copy_filesystem(
                source_filesystem.open_async(i.path, "rb", compression=i.compression),
                stage_fs.open_async(o.path, "wb", compression=o.compression),
                block_size=block_size,
            )

        for i, o in resolved_names:
            logger.info(f"Copying to {o.path}")
            await copy_filesystem(
                stage_fs.open_async(o.path, "rb"),
                target_filesystem.open_async(o.path, "wb"),
                block_size,
            )

        logger.info(f"Copied {len(resolved_names)} items")
        return [(i.path, o.path) for i, o in resolved_names]


def _expand(t1, t2):
    """
    Helper function to flatten input tuples
    :param t1:
    :param t2:
    :return:
    """
    return t1[0], t1[1], t2[0], t2[1]


def _as_iterable(datasource):
    """
    Transforms datasource into an iterable list
    :param datasource:
    :return:
    """
    return (
        [{}]
        if datasource is None
        else [datasource]
        if isinstance(datasource, dict)
        else tuple(datasource)
    )
