import json
import uuid
from os import path
from tempfile import TemporaryDirectory

from prefect_filesystem.abstract_local_filesystem import AbstractLocalFileSystem
from prefect_filesystem.compression import named_unzip
from prefect_filesystem.tasks import filesystem_copy, filesystem_get, filesystem_put


class TempIt:
    def __init__(self):
        self.dir = TemporaryDirectory()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dir.__exit__(exc_type, exc_val, exc_tb)

    def get_file(self):
        return path.join(self.dir.name, str(uuid.uuid1()))

    def get_local_filesystem(self):
        return AbstractLocalFileSystem(root_path=self.dir.name)

    @staticmethod
    def get_filename():
        return str(uuid.uuid1())

    def read_file(self, name, mode="rt"):
        with open(path.join(self.dir.name, name), mode) as fd:
            return fd.read()

    def read_json(self, name, mode="rt"):
        with open(path.join(self.dir.name, name), mode) as fd:
            return json.load(fd)

    @staticmethod
    def data_block():
        return b"test_block"


def test_unzip_named_filename():

    with TempIt() as tmp:
        file = tmp.get_file()
        data = tmp.data_block()

        with open(file, "wb") as fp:
            with named_unzip(fp, "w", "file1.txt") as xx:
                xx.write(data)

        with open(file, "rb") as fp:
            with named_unzip(fp, "r", "file1.txt") as xx:
                new_data = xx.read()
                print(new_data)

        assert new_data == data


def test_unzip_unnamed_filename():
    with TempIt() as tmp:
        file = tmp.get_file()
        data = tmp.data_block()

        with open(file, "wb") as fp:
            with named_unzip(fp, "w") as xx:
                xx.write(data)

        with open(file, "rb") as fp:
            with named_unzip(fp, "r") as xx:
                new_data = xx.read()
                print(new_data)

        assert new_data == data


async def test_local_put_string(prefect_disable_logging):
    with TempIt() as tmp:
        lfs = tmp.get_local_filesystem()
        file = tmp.get_filename()
        content = "my_content" * 2048

        await filesystem_put.fn(content=content, filename=file, filesystem=lfs)

        new_data = tmp.read_file(file)

        assert content == new_data


async def test_local_put_json(prefect_disable_logging):
    with TempIt() as tmp:
        lfs = tmp.get_local_filesystem()
        file = tmp.get_filename()
        content = [{"a": "my_content", "b": 2, "c": i} for i in range(1000)]

        await filesystem_put.fn(content=content, filename=file, filesystem=lfs)

        new_data = tmp.read_json(file)

        assert content == new_data


async def test_local_put_binary(prefect_disable_logging):
    with TempIt() as tmp:
        lfs = tmp.get_local_filesystem()
        file = tmp.get_filename()
        content = b"binary content" * 1024 * 1024

        await filesystem_put.fn(content=content, filename=file, filesystem=lfs)

        new_data = tmp.read_file(file, "rb")

        assert content == new_data


async def test_local_put_get_string(prefect_disable_logging):
    with TempIt() as tmp:
        lfs = tmp.get_local_filesystem()
        file = tmp.get_filename()
        content = "my_content" * 1024 * 1024

        await filesystem_put.fn(content=content, filename=file, filesystem=lfs)
        new_data = await filesystem_get.fn(filename=file, filesystem=lfs)
        assert content == new_data


async def test_local_put_get_string_transform(prefect_disable_logging):
    with TempIt() as tmp:
        lfs = tmp.get_local_filesystem()
        file = tmp.get_filename()
        content = "my_content" * 1024 * 1024

        await filesystem_put.fn(content=content, filename=file, filesystem=lfs)
        new_data = await filesystem_get.fn(
            filename=file, filesystem=lfs, transform=_transform
        )
        assert _transform(content) == new_data


def _transform(x):
    return f"_{x}_"


async def test_local_put_get_json(prefect_disable_logging):
    with TempIt() as tmp:
        lfs = tmp.get_local_filesystem()
        file = tmp.get_filename()
        content = {"a": "my_content", "b": 2, "c": 3}
        await filesystem_put.fn(content=content, filename=file, filesystem=lfs)
        new_data = await filesystem_get.fn(
            filename=file, filesystem=lfs, transform="json"
        )
        assert content == new_data


async def test_local_put_get_binary(prefect_disable_logging):
    with TempIt() as tmp:
        lfs = tmp.get_local_filesystem()
        file = tmp.get_filename()
        content = b"binary content"
        await filesystem_put.fn(content=content, filename=file, filesystem=lfs)
        new_data = await filesystem_get.fn(filename=file, filesystem=lfs, encoding=None)
        assert content == new_data


async def test_local_put_get_json_compress_gzip(prefect_disable_logging):
    with TempIt() as tmp:
        lfs = tmp.get_local_filesystem()
        file = tmp.get_filename()
        compression = "gzip"
        content = [{"a": "my_content", "b": 2, "c": i} for i in range(1000)]
        await filesystem_put.fn(
            content=content, filename=file, filesystem=lfs, compression=compression
        )
        new_data = await filesystem_get.fn(
            filename=file, filesystem=lfs, transform="json", compression=compression
        )
        assert content == new_data


async def test_local_put_get_json_compress_zip(prefect_disable_logging):
    with TempIt() as tmp:
        lfs = tmp.get_local_filesystem()
        file = tmp.get_filename()
        compression = "zip"
        content = [{"a": "my_content", "b": 2, "c": i} for i in range(1000)]
        await filesystem_put.fn(
            content=content, filename=file, filesystem=lfs, compression=compression
        )
        new_data = await filesystem_get.fn(
            filename=file, filesystem=lfs, transform="json", compression=compression
        )
        assert content == new_data


async def test_local_copy(prefect_disable_logging):
    with TempIt() as tmp:
        lfs = tmp.get_local_filesystem()
        lfs2 = tmp.get_local_filesystem()
        file = tmp.get_filename()
        file2 = tmp.get_filename()
        compression = "zip"
        content = [{"a": "my_content", "b": 2, "c": i} for i in range(1000)]
        await filesystem_put.fn(
            content=content, filename=file, filesystem=lfs, compression=compression
        )

        await filesystem_copy.fn(
            source_filename=file,
            source_filesystem=lfs,
            target_filename=file2,
            target_filesystem=lfs2,
        )

        new_data = await filesystem_get.fn(
            filename=file2, filesystem=lfs2, transform="json", compression=compression
        )
        assert content == new_data


async def test_local_copy_compression_zip(prefect_disable_logging):
    """Decompress during the copy"""
    with TempIt() as tmp:
        lfs = tmp.get_local_filesystem()
        lfs2 = tmp.get_local_filesystem()
        file = tmp.get_filename()
        file2 = tmp.get_filename()
        compression = "zip"
        content = {"a": "my_content", "b": 2, "c": 3}
        await filesystem_put.fn(
            content=content, filename=file, filesystem=lfs, compression=compression
        )

        await filesystem_copy.fn(
            source_filename=file,
            source_filesystem=lfs,
            source_compression=compression,
            target_filename=file2,
            target_filesystem=lfs2,
        )

        new_data = await filesystem_get.fn(
            filename=file2, filesystem=lfs2, transform="json"
        )
        assert content == new_data


async def test_local_copy_compression_zip_ex(prefect_disable_logging):
    """Decompress during the copy"""
    with TempIt() as tmp:
        lfs = tmp.get_local_filesystem()
        lfs2 = tmp.get_local_filesystem()
        file = tmp.get_filename()
        file2 = tmp.get_filename()
        compression = "zip_ex"
        content = {"a": "my_content", "b": 2, "c": 3}
        await filesystem_put.fn(
            content=content, filename=file, filesystem=lfs, compression=compression
        )

        await filesystem_copy.fn(
            source_filename=file,
            source_filesystem=lfs,
            source_compression=compression,
            target_filename=file2,
            target_filesystem=lfs2,
        )

        new_data = await filesystem_get.fn(
            filename=file2, filesystem=lfs2, transform="json"
        )
        assert content == new_data
