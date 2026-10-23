"""Bound external page data before parsing or transferring it into Python."""

import zlib

import brotli

from ..config import settings


class ResourceLimitError(ValueError):
    """A page exceeded a resource budget; this is not a selector failure."""


def enforce_text_limit(content: str) -> str:
    if len(content) > settings.extraction_text_max_chars:
        raise ResourceLimitError("extraction_text_limit")
    return content


async def read_page_bytes(response, max_bytes=None) -> bytes:
    """Read raw HTTP chunks with bounded wire and decompressed sizes.

    Callers must request auto_decompress=False. Content-Length is deliberately
    not trusted. Decoders receive an output budget before allocating output.
    """
    limit = settings.extraction_http_max_bytes if max_bytes is None else max_bytes
    if limit <= 0:
        raise ValueError("Page byte limit must be positive")
    encoding = response.headers.get("Content-Encoding", "identity").lower().strip()
    if encoding in ("", "identity"):
        decoder = None
    elif encoding == "gzip":
        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    elif encoding == "deflate":
        decoder = zlib.decompressobj()
    elif encoding == "br":
        decoder = brotli.Decompressor()
    else:
        response.close()
        raise ResourceLimitError("unsupported_content_encoding")

    body = bytearray()
    wire_bytes = 0

    def append(chunk):
        if len(body) + len(chunk) > limit:
            raise ResourceLimitError("extraction_http_limit")
        body.extend(chunk)

    try:
        async for chunk in response.content.iter_chunked(64 * 1024):
            wire_bytes += len(chunk)
            if wire_bytes > limit:
                raise ResourceLimitError("extraction_wire_limit")
            if decoder is None:
                append(chunk)
            elif encoding == "br":
                append(decoder.process(chunk, output_buffer_limit=limit - len(body) + 1))
                while not decoder.can_accept_more_data():
                    append(decoder.process(b"", output_buffer_limit=limit - len(body) + 1))
            else:
                append(decoder.decompress(chunk, limit - len(body) + 1))
                while decoder.unconsumed_tail:
                    append(decoder.decompress(decoder.unconsumed_tail, limit - len(body) + 1))
                if decoder.unused_data:
                    raise ValueError("Unexpected trailing compressed page data")
        if decoder is not None:
            finished = decoder.is_finished() if encoding == "br" else decoder.eof
            if not finished:
                raise ValueError("Truncated compressed page")
        return bytes(body)
    except BaseException:
        response.close()
        raise


async def read_page_text(response, max_bytes=None) -> str:
    data = await read_page_bytes(response, max_bytes=max_bytes)
    charset = response.charset or "utf-8"
    try:
        return data.decode(charset, errors="replace")
    except LookupError:
        return data.decode("utf-8", errors="replace")
