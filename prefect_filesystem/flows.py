"""This is an example flows module"""
from prefect import flow

from prefect_filesystem.blocks import FilesystemBlock
from prefect_filesystem.tasks import (
    goodbye_prefect_filesystem,
    hello_prefect_filesystem,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    FilesystemBlock.seed_value_for_example()
    block = FilesystemBlock.load("sample-block")

    print(hello_prefect_filesystem())
    print(f"The block's value: {block.value}")
    print(goodbye_prefect_filesystem())
    return "Done"


if __name__ == "__main__":
    hello_and_goodbye()
