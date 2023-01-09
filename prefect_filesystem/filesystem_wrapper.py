"""
Provides a wrapper
"""
from .abstract_block import AbstractBlock


class AbstractWrapper(AbstractBlock):
    """
    Wrapper for any simple Prefect Blocks
    """

    def __init__(self, block, **kwargs):
        self.block = block
        self._fs = None
        super().__init__(**kwargs)

    @property
    def filesystem(self):
        """
        Wrapped pass through
        :return:
        """
        return self.block.filesystem

    @property
    def basepath(self):
        """
        Wrapped passthrough
        :return:
        """
        return self.block.basepath
