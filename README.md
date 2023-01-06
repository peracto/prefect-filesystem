# prefect-filesystem

Visit the full docs [here](https://peracto.github.io/prefect-filesystem) to see additional examples and the API reference.

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-filesystem/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-filesystem?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/peracto/prefect-filesystem/" alt="Stars">
        <img src="https://img.shields.io/github/stars/peracto/prefect-filesystem?color=0052FF&labelColor=090422" /></a>
    <a href="https://pepy.tech/badge/prefect-filesystem/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-filesystem?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/peracto/prefect-filesystem/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/peracto/prefect-filesystem?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

## Welcome!

Collection of Prefect tasks to simplfy FifleSystem actions

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-filesystem` with `pip`:

```bash
pip install prefect-filesystem
```

A list of available blocks in `prefect-filesystem` and their setup instructions can be found [here](https://peracto.github.io/prefect-filesystem/#blocks-catalog).

### Write and run a flow

```python
from prefect import flow
from prefect_filesystem.tasks import (
    goodbye_prefect_filesystem,
    hello_prefect_filesystem,
)

# Use `with_options` to customize options on any existing task or flow

custom_goodbye_prefect_filesystem = goodbye_prefect_filesystem.with_options(
    name="My custom task name",
    retries=2,
    retry_delay_seconds=10,
)

@flow
def example_flow():
    hello_prefect_filesystem
    custom_goodbye_prefect_filesystem

example_flow()
```

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://orion-docs.prefect.io/collections/usage/)!

## Resources

If you encounter any bugs while using `prefect-filesystem`, feel free to open an issue in the [prefect-filesystem](https://github.com/peracto/prefect-filesystem) repository.

If you have any questions or issues while using `prefect-filesystem`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to star or watch [`prefect-filesystem`](https://github.com/peracto/prefect-filesystem) for updates too!

## Contributing

If you'd like to help contribute to fix an issue or add a feature to `prefect-filesystem`, please [propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

Here are the steps:
1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository)
2. [Clone the forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository)
3. Install the repository and its dependencies:
```
pip install -e ".[dev]"
```
4. Make desired changes
5. Add tests
6. Insert an entry to [CHANGELOG.md](https://github.com/peracto/prefect-filesystem/blob/main/CHANGELOG.md)
7. Install `pre-commit` to perform quality checks prior to commit:
```
pre-commit install
```
8. `git commit`, `git push`, and create a pull request
