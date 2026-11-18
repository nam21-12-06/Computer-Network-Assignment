"""Peer va vong doi ket noi voi tracker."""

import asyncio
import logging
from typing import List
from .peer_modules.client import TrackerClient, TrackerClientError
from .peer_modules.protocol import MAX_FRAME_BYTES, FrameMixin
from .peer_modules.connections import ConnectionMixin

LOGGER = logging.getLogger(__name__)


class PeerNode(FrameMixin, ConnectionMixin):
    """Dieu phoi listener, heartbeat va cac ket noi truc tiep."""

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
        self.connections = {}
        self.incoming = asyncio.Queue()
        self._tasks = set()
        self._heartbeat_task = None
        self._closing = False

    def _spawn(self, coroutine):
        task = asyncio.ensure_future(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

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

    async def discover(self) -> List[dict]:
        result = await self.tracker.post("/tracker/peers", {"peer_id": self.peer_id})
        return result["peers"]

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
