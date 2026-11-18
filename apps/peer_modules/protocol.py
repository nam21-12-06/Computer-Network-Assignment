"""Dong khung JSON va dong TCP stream."""

import asyncio
from contextlib import suppress
import json

MAX_FRAME_BYTES = 65536


async def _close_writer(writer):
    """Python 3.6 chua co StreamWriter.wait_closed()."""
    writer.close()
    wait_closed = getattr(writer, "wait_closed", None)
    if wait_closed is not None:
        with suppress(OSError):
            await wait_closed()


class FrameMixin:
    @staticmethod
    async def _read_frame(reader: asyncio.StreamReader) -> dict:
        # TCP la dong byte: newline la ranh gioi frame, khong phu thuoc recv().
        data = await reader.readline()
        if not data:
            raise EOFError("peer da dong ket noi")
        if len(data) > MAX_FRAME_BYTES or not data.endswith(b"\n"):
            raise ValueError("frame qua lon hoac khong day du")
        frame = json.loads(data.decode("utf-8"))
        if not isinstance(frame, dict):
            raise ValueError("frame phai la JSON object")
        return frame

    @staticmethod
    async def _write_frame(writer: asyncio.StreamWriter, frame: dict) -> None:
        data = json.dumps(frame, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        if len(data) > MAX_FRAME_BYTES:
            raise ValueError("frame qua lon")
        writer.write(data)
        await writer.drain()
