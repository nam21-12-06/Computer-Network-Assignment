"""Thiet lap va quan ly ket noi giua cac peer."""

import asyncio
import logging
from .protocol import MAX_FRAME_BYTES, _close_writer

LOGGER = logging.getLogger(__name__)


class ConnectionMixin:
    async def connect_peer(self, target_peer_id: str) -> None:
        """Lay endpoint tu tracker, sau do mo ket noi thang den peer dich."""
        if self.server is None or self._closing:
            raise RuntimeError("peer chua khoi dong")
        if target_peer_id in self.connections:
            return
        result = await self.tracker.post("/tracker/connect-peer", {
            "peer_id": self.peer_id, "target_peer_id": target_peer_id,
        })
        target = result["target"]
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(target["ip"], target["listen_port"], limit=MAX_FRAME_BYTES + 1),
            timeout=5,
        )
        try:
            await self._write_frame(writer, {"type": "hello", "peer_id": self.peer_id})
            reply = await asyncio.wait_for(self._read_frame(reader), timeout=5)
            if reply.get("type") != "hello" or reply.get("peer_id") != target_peer_id:
                raise ValueError("peer dich tra ve handshake khong hop le")
            if target_peer_id in self.connections:
                raise ConnectionError("da co ket noi toi peer nay")
            self.connections[target_peer_id] = writer
            self._spawn(self._receive_loop(target_peer_id, reader, writer))
        except BaseException:
            await _close_writer(writer)
            raise

    def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._spawn(self._accept_handshake(reader, writer))

    async def _accept_handshake(self, reader, writer) -> None:
        peer_id = None
        try:
            hello = await asyncio.wait_for(self._read_frame(reader), timeout=5)
            peer_id = hello.get("peer_id")
            if hello.get("type") != "hello" or not isinstance(peer_id, str) or not peer_id or peer_id == self.peer_id:
                raise ValueError("handshake khong hop le")
            if self._closing or peer_id in self.connections:
                raise ConnectionError("peer da ket noi hoac dang dong")
            # Luu truoc await de hai handshake song song khong ghi de nhau.
            self.connections[peer_id] = writer
            await self._write_frame(writer, {"type": "hello", "peer_id": self.peer_id})
            await self._receive_loop(peer_id, reader, writer)
        except (EOFError, ValueError, UnicodeError, ConnectionError, OSError, asyncio.TimeoutError):
            LOGGER.info("Ket noi den bi dong hoac handshake that bai")
        finally:
            if self.connections.get(peer_id) is writer:
                del self.connections[peer_id]
            await _close_writer(writer)

    async def _receive_loop(self, peer_id, reader, writer) -> None:
        try:
            while True:
                frame = await self._read_frame(reader)
                await self.incoming.put((peer_id, frame))
        except (EOFError, ValueError, UnicodeError, ConnectionError, OSError):
            pass
        finally:
            if self.connections.get(peer_id) is writer:
                del self.connections[peer_id]
            await _close_writer(writer)

    async def send(self, target_peer_id: str, frame: dict) -> None:
        """Gui frame ung dung qua ket noi P2P da duoc thiet lap."""
        writer = self.connections.get(target_peer_id)
        if writer is None:
            raise ConnectionError("chua ket noi toi peer")
        await self._write_frame(writer, frame)

    async def disconnect_peer(self, peer_id: str) -> None:
        writer = self.connections.pop(peer_id, None)
        if writer is not None:
            await _close_writer(writer)
