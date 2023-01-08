"""
Abstract Local Filesystem
"""

from contextlib import contextmanager
from os.path import join
from tempfile import TemporaryDirectory

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem as FsSpecLocalFileSystem
from pydantic import BaseModel, Field

from prefect_filesystem.abstract_block import AbstractBlock


class AbstractLocalFileSystem(AbstractBlock, BaseModel):
    """
    Implements Abstract Local File System block
    """

    _block_type_name = "file"
    _logo_url = (
        "https://images.ctfassets.net/gm98wzqotmnx"
        "/1jbV4lceHOjGgunX15lUwT/db88e184d727f721575aeb054a37e277/aws.png?h=250"
    )

    root_path: str = Field(
        default=...,
        description="Base Path.",
        example="/tmp",
    )

    auto_mkdir: bool = Field(
        default=False,
        description="Automatically make directories if set",
    )

    def _resolve_abstract_filesystem(self) -> AbstractFileSystem:
        """
        Resolves the provided filesystem into an AbstractFileSystem
        :return: AbstractFileSystem
        """
        """
        Resolves the provided filesystem into an AbstractFileSystem
        :return: AbstractFileSystem
        """
        return FsSpecLocalFileSystem(auto_mkdir=self.auto_mkdir)

    def build_path(self, path: str) -> str:
        """
        Joins path to the basepath trimming any trailing '/'
        :param path:
        :return:
        """
        return join(f"file://{self.root_path}", path.lstrip("/"))

    @classmethod
    @contextmanager
    def make_temp(cls, **kwargs):
        """
        Helper function to create a Local File System that is linked to a
        temporary file source

        Args:
            **kwargs:

        Returns:

        """

        with TemporaryDirectory() as tmp_dir:
            yield cls(root_path=tmp_dir, **kwargs)
