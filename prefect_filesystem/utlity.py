import sys
from collections import namedtuple
from inspect import isawaitable

from prefect.blocks.core import Block

from prefect_filesystem.abstract_block import AbstractBlock
from prefect_filesystem.filesystem_wrapper import AbstractWrapper

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict


PathFormat = namedtuple("PathFormat", ("path", "compression"))


def apply_path_format(
    data, path, compression=None, field_name="filename"
) -> PathFormat:
    """
    Formats any placeholder in filename and compression strings

    :param data:
    :param path:
    :param compression:
    :param field_name:
    :return: PathFormat
    """
    if not isinstance(data, dict):
        return path, compression
    filename = compression.get(field_name) if isinstance(compression, dict) else ""
    return PathFormat(
        path.format(**data),
        {**compression, field_name: filename.format(**data)}
        if "{" in filename
        else compression,
    )


async def copy_filesystem(source, target, block_size=1024 * 1024):
    """
    Copies binary data from source to the target in the specified block_size

    Args:
        source:
        target:
        block_size:

    Returns:

    """
    s = await source if isawaitable(source) else source
    t = await target if isawaitable(target) else target

    try:
        while True:
            _bytes = await s.read(block_size)
            if not _bytes:
                break
            await t.write(_bytes)
    finally:
        if s != source:
            await s.aclose()
        if t != target:
            await t.aclose()


class CompressionType(TypedDict, total=False):
    """
    PLacehold interface to describe Compression dictionary
    """

    type: str
    filename: str


def ensure_abstract(block) -> AbstractBlock:
    """
    If block is not a subclass of AbstractBlock, then we wrap it
    :param block:
    :return:
    """
    if isinstance(block, AbstractBlock):
        return block
    if isinstance(block, Block):
        return AbstractWrapper(block)

    raise Exception("Expected Abstract or Block type")
