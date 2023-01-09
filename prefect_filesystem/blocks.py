"""
Blocks
"""
from typing import Optional

from prefect.filesystems import (
    RemoteFileSystem,
    WritableDeploymentStorage,
    WritableFileSystem,
)
from prefect.utilities.asyncutils import sync_compatible
from pydantic import Field, SecretStr


class Sftp(WritableFileSystem, WritableDeploymentStorage):
    """
    Store data as a file on SFTP server.

    Example:
        Load stored Sftp config:
        ```python
        sftp_block = Sftp.load("BLOCK_NAME")
        ```
    """

    _block_type_name = "sftp"
    _logo_url = (
        "https://images.ctfassets.net/gm98wzqotmnx/"
        "EVKjxM7fNyi4NGUSkeTEE/95c958c5dd5a56c59ea5033e919c1a63/image1.png?h=250"
    )

    host: str = Field(
        ...,
        title="Host name",
        description="the server to connect to",
        example="mytfp.host.com",
    )

    port: Optional[int] = Field(22, title="port", description="port (default: 22).")

    username: Optional[str] = Field(
        None,
        title="Username",
        description="the username to authenticate as "
        "(defaults to the current local username)",
    )

    password: Optional[SecretStr] = Field(
        None,
        title="Password",
        description=(
            "Used for password authentication; is also used for private key "
            "decryption if ``passphrase`` is not given.."
        ),
    )

    pkey: Optional[SecretStr] = Field(
        None,
        title="pkey",
        description="an optional private key to use for authentication",
    )

    key_filename: Optional[str] = Field(
        None,
        title="key_filename",
        description=(
            "the filename, or list of filenames, of optional private "
            "key(s) and/or certs to try for authentication"
        ),
    )

    timeout: Optional[float] = Field(
        None,
        title="timeout",
        description="an optional timeout (in seconds) for the TCP connect",
    )

    allow_agent: Optional[bool] = Field(
        None,
        title="Allow Agent",
        description="set to False to disable connecting to the SSH agent",
    )

    look_for_keys: Optional[bool] = Field(
        None,
        title="Look for Keys",
        description="set to False to disable searching for discoverable "
        "private key files in ``~/.ssh/``",
    )

    compress: Optional[bool] = Field(
        None, title="Compress", description="set to True to turn on compression"
    )

    folder_root: Optional[str] = Field(
        None, title="Folder Root", description="Root Folder"
    )

    _remote_file_system: RemoteFileSystem = None

    @property
    def basepath(self) -> str:
        """
        Builds basepath from host and folder_root
        Returns
        -------

        """
        return (
            f"sftp://{self.host}"
            if not self.folder_root
            else f"sftp://{self.host}/{self.folder_root}"
        )

    @property
    def filesystem(self) -> RemoteFileSystem:
        """
        Creates RemoveFileSystem from block properties
        Returns
        -------

        """
        settings = dict(
            host=self.host,
            port=self.port,
            username=self.username,
            password=_get_secret(self.password),
            pkey=_get_secret(self.pkey),
            key_filename=self.key_filename,
            timeout=self.timeout,
            allow_agent=self.allow_agent,
            look_for_keys=self.look_for_keys,
            compress=self.compress,
        )

        self._remote_file_system = RemoteFileSystem(
            basepath=self.basepath,
            settings={k: v for k, v in settings.items() if v is not None},
        )
        return self._remote_file_system

    @sync_compatible
    async def get_directory(
        self, from_path: Optional[str] = None, local_path: Optional[str] = None
    ):
        """
        Downloads a directory from a given remote path to a local directory.

        Defaults to downloading the entire contents of the block's basepath
        to the current working directory.
        """
        return await self.filesystem.get_directory(
            from_path=from_path, local_path=local_path
        )

    @sync_compatible
    async def put_directory(
        self,
        local_path: Optional[str] = None,
        to_path: Optional[str] = None,
        ignore_file: Optional[str] = None,
    ) -> int:
        """
        Uploads a directory from a given local path to a remote directory.

        Defaults to uploading the entire contents of the current working directory
        to the block's basepath.
        """
        return await self.filesystem.put_directory(
            local_path=local_path, to_path=to_path, ignore_file=ignore_file
        )

    @sync_compatible
    async def read_path(self, path: str) -> bytes:
        """
        Read file from filesystem with the provided path
        Parameters
        ----------
        path

        Returns
        -------

        """
        return await self.filesystem.read_path(path)

    @sync_compatible
    async def write_path(self, path: str, content: bytes) -> str:
        """
        Write content to the path in within the filesystem
        Parameters
        ----------
        path
        content

        Returns
        -------

        """
        return await self.filesystem.write_path(path=path, content=content)


def _get_secret(s):
    return None if s is None else s.get_secret_value()
