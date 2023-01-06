from prefect import flow

from prefect_filesystem.tasks import (
    goodbye_prefect_filesystem,
    hello_prefect_filesystem,
)


def test_hello_prefect_filesystem():
    @flow
    def test_flow():
        return hello_prefect_filesystem()

    result = test_flow()
    assert result == "Hello, prefect-filesystem!"


def goodbye_hello_prefect_filesystem():
    @flow
    def test_flow():
        return goodbye_prefect_filesystem()

    result = test_flow()
    assert result == "Goodbye, prefect-filesystem!"
