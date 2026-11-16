"""Peer discovery va ket noi P2P bat dong bo su dung TCP stream.

Tracker chi dung cho buoc dang ky va tim dia chi. Sau handshake, du lieu
duoc gui truc tiep giua hai peer, khong di qua tracker. API send/receive duoc
de doc lap de nhom messaging co the thay protocol ma khong doi tracker.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
import json
import logging
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


LOGGER = logging.getLogger(__name__)
MAX_FRAME_BYTES = 65536


class TrackerClientError(Exception):
    """Tracker khong tra ve ket qua thanh cong."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class TrackerClient:
    """HTTP client duoc dua vao thread de khong chan asyncio event loop."""

    def __init__(self, base_url: str, timeout: float = 5.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _post(self, path: str, payload: dict) -> dict:
        request = Request(
            self.base_url + path,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise TrackerClientError(detail, error.code) from error
        except (OSError, URLError, ValueError) as error:
            raise TrackerClientError(str(error)) from error

    async def post(self, path: str, payload: dict) -> dict:
        return await asyncio.to_thread(self._post, path, payload)


class PeerNode:
    """Quan ly vong doi peer, heartbeat va cac ket noi truc tiep.

    Moi frame la mot doi tuong JSON tren mot dong UTF-8. Frame hello duoc
    dung rieng de hai dau trao doi peer_id; frame con lai thuoc ung dung chat.
    Giao thuc nay la de xuat cho nhom, chua thay the authentication cua nhom.
    """

    def __init__(self, peer_id: str, user_id: str, tracker: TrackerClient,
                 listen_ip: str = "0.0.0.0", listen_port: int = 0,
                 heartbeat_interval: float = 20.0):
        if not peer_id or not user_id or heartbeat_interval <= 0:
            raise ValueError("peer_id, user_id va heartbeat_interval phai hop le")
        self.peer_id = peer_id
        self.user_id = user_id
        self.tracker = tracker
        self.listen_ip = listen_ip
        self.listen_port = listen_port
        self.heartbeat_interval = heartbeat_interval
        self.server = None
        self.connections: dict[str, asyncio.StreamWriter] = {}
        self.incoming: asyncio.Queue[tuple[str, dict]] = asyncio.Queue()
        self._tasks: set[asyncio.Task] = set()
        self._heartbeat_task = None
        self._closing = False

    def _spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

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

    async def start(self) -> None:
        """Mo cong lang nghe truoc khi cong bo dia chi cho tracker."""
        if self.server is not None:
            raise RuntimeError("peer da khoi dong")
        self._closing = False
        self.server = await asyncio.start_server(
            self._accept, self.listen_ip, self.listen_port, limit=MAX_FRAME_BYTES + 1
        )
        self.listen_port = self.server.sockets[0].getsockname()[1]
        try:
            await self._register()
        except BaseException:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
            raise
        self._heartbeat_task = self._spawn(self._heartbeat_loop())

    async def _register(self) -> None:
        await self.tracker.post("/tracker/register", {
            "peer_id": self.peer_id, "user_id": self.user_id,
            "listen_port": self.listen_port,
        })

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            try:
                await self.tracker.post("/tracker/heartbeat", {
                    "peer_id": self.peer_id, "listen_port": self.listen_port,
                })
            except TrackerClientError as error:
                # Neu tracker khoi dong lai va quen peer, dang ky lai sau 404.
                if error.status == 404:
                    try:
                        await self._register()
                    except TrackerClientError:
                        LOGGER.warning("Khong the dang ky lai voi tracker", exc_info=True)
                else:
                    LOGGER.warning("Heartbeat khong thanh cong: %s", error)

    async def discover(self) -> list[dict]:
        result = await self.tracker.post("/tracker/peers", {"peer_id": self.peer_id})
        return result["peers"]

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
            writer.close()
            await writer.wait_closed()
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
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

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
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    async def send(self, target_peer_id: str, frame: dict) -> None:
        """Gui frame ung dung qua ket noi P2P da duoc thiet lap."""
        writer = self.connections.get(target_peer_id)
        if writer is None:
            raise ConnectionError("chua ket noi toi peer")
        await self._write_frame(writer, frame)

    async def disconnect_peer(self, peer_id: str) -> None:
        writer = self.connections.pop(peer_id, None)
        if writer is not None:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    async def close(self) -> None:
        """Ngung nhan ket noi, dong peer, huy heartbeat va bao tracker."""
        if self.server is None:
            return
        self._closing = True
        self.server.close()
        await self.server.wait_closed()
        self.server = None
        for peer_id in list(self.connections):
            await self.disconnect_peer(peer_id)
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        try:
            await self.tracker.post("/tracker/disconnect", {"peer_id": self.peer_id})
        except TrackerClientError as error:
            # Neu tracker mat ket noi, TTL se danh dau peer inactive sau do.
            LOGGER.warning("Khong the thong bao disconnect: %s", error)
