"""压缩格式注册表。

两种格式均为公开标准、由 Python 标准库支持, 本仓库不自创任何压缩算法:
  - gzip (RFC 1952, DEFLATE)  —— 旧格式 v1, 历史分区使用
  - xz   (LZMA2, .xz 容器)    —— 新格式 v2, 新写入使用

gzip 压缩固定 mtime=0, xz 固定 preset+check, 保证同样内容得到同样字节,
校验值才可登记、可核对。xz 容器内同时启用 SHA-256 校验, 格式自身也有完整性保护。
"""
from __future__ import annotations

import gzip
import lzma

OLD_FORMAT = "gzip"
NEW_FORMAT = "xz"


def _gzip_compress(data: bytes) -> bytes:
    return gzip.compress(data, compresslevel=6, mtime=0)


def _xz_compress(data: bytes) -> bytes:
    return lzma.compress(data, format=lzma.FORMAT_XZ, check=lzma.CHECK_SHA256, preset=6)


FORMATS = {
    "gzip": {
        "ext": "gz",
        "label": "gzip (旧格式 v1)",
        "compress": _gzip_compress,
        "decompress": gzip.decompress,
    },
    "xz": {
        "ext": "xz",
        "label": "xz/LZMA2 (新格式 v2)",
        "compress": _xz_compress,
        "decompress": lzma.decompress,
    },
}
