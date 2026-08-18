"""Shared wire format for the local test harness.

Mirrors the default [framing] block in ../config.toml:
    2-byte big-endian length prefix, length excludes the prefix itself.

The body is a pipe-delimited stand-in for ISO 8583. It carries a STAN so a
request can be matched to its response, and the harness asserts the body comes
back byte-identical -- that is what proves the interceptor is not corrupting or
reordering the stream.
"""

import socket
import struct

PREFIX = struct.Struct(">H")
PREFIX_LEN = PREFIX.size
MAX_FRAME = 65536


def encode(body: bytes) -> bytes:
    if len(body) > MAX_FRAME:
        raise ValueError(f"body {len(body)} exceeds max frame {MAX_FRAME}")
    return PREFIX.pack(len(body)) + body


def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes or raise. Plain recv() is free to return short."""
    chunks = []
    remaining = n
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError(f"peer closed with {remaining} of {n} bytes outstanding")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_frame(sock: socket.socket) -> bytes:
    (length,) = PREFIX.unpack(recv_exact(sock, PREFIX_LEN))
    if length == 0 or length > MAX_FRAME:
        raise ValueError(f"implausible frame length {length} -- stream desynced")
    return recv_exact(sock, length)


def build_request(stan: int, conn: int, pad: int = 0) -> bytes:
    body = (
        f"msgType=request,txnRef={stan:08d},connRef={conn:04d}"
        f",accountName=Zacky,accountNumber=11020134353,bankCode=1234"
        f",amount={(stan % 90000) + 100:012d},currency=360"
    ).encode()
    if pad > len(body):
        # Pad with a real field rather than filler bytes, so a padded frame is
        # still parseable by the interceptor's [parse] stage.
        body += b",filler=" + b"x" * max(1, pad - len(body) - 8)
    return body


def to_response(request_body: bytes) -> bytes:
    """FMS reply: flip msgType and append a result, echo everything else verbatim.

    Echoing the rest unchanged is deliberate -- it lets the client verify the
    return path byte-for-byte too, not just the request path.
    """
    if request_body.startswith(b"msgType=request"):
        return (
            b"msgType=response" + request_body[len(b"msgType=request"):]
            + b",status=APPROVED,responseCode=00"
        )
    # Anything else (e.g. the status.sh health probe) is echoed with a result
    # appended. Appending rather than prefixing avoids emitting a duplicate
    # `msgType` key, which would otherwise show up ambiguously in Kafka.
    return request_body + b",echo=1,responseCode=00"


def parse_stan(body: bytes) -> int:
    for field in body.split(b","):
        if field.startswith(b"txnRef="):
            return int(field[7:])
    raise ValueError(f"no txnRef in {body[:64]!r}")
