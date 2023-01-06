"""
Providers enhanced file compression wrappers
"""

from datetime import datetime
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


def named_unzip(
    infile,
    mode,
    filename=None,
    compression_type=ZIP_DEFLATED,
    force_zip64=True,
    **kwargs
):
    """
    Return file-like object for archive file 'filename' within the file provide
    :param infile:
    :param mode:
    :param filename:
    :param compression_type:
    :param force_zip64:
    :param kwargs:
    :return:
    """

    if "r" not in mode:
        filename = filename or "file"
        zip_file = ZipFile(infile, "w", compression_type, **kwargs)
        zip_info = ZipInfo(filename, datetime.now().timetuple()[:6])
        zip_info.compress_type = zip_file.compression
        fo = zip_file.open(zip_info, "w", force_zip64=force_zip64)
        fo.close = lambda closer=fo.close: closer() or zip_file.close()
        return fo

    zip_file = ZipFile(infile)
    filename = filename or zip_file.namelist()[0]
    return zip_file.open(filename, "r", **kwargs)


compr = {"zip_ex": named_unzip}
