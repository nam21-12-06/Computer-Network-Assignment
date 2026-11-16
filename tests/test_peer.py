"""Kiem tra hai peer tao ket noi TCP that tren loopback."""

import asyncio
import unittest

from apps.peer import PeerNode, TrackerClientError
from apps.tracker import TrackerError, TrackerService


class InMemoryTrackerClient:
    """Mo phong HTTP de test vong doi peer tach biet raw HTTP backend."""

    def __init__(self):
        self.service = TrackerService()
        self.heartbeats = 0

    async def post(self, path, data):
        try:
            if path == "/tracker/register":
                return {"peer": self.service.register_peer(data["peer_id"], data["user_id"], "127.0.0.1", data["listen_port"])}
            if path == "/tracker/heartbeat":
                self.heartbeats += 1
                return {"peer": self.service.heartbeat(data["peer_id"])}
            if path == "/tracker/peers":
                return {"peers": self.service.list_active_peers(data["peer_id"])}
            if path == "/tracker/connect-peer":
                return {"target": self.service.connect_peer(data["peer_id"], data["target_peer_id"])}
            if path == "/tracker/disconnect":
                return {"peer": self.service.disconnect_peer(data["peer_id"])}
        except TrackerError as error:
            raise TrackerClientError(str(error), error.status_code) from error
        raise AssertionError(path)


class PeerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tracker = InMemoryTrackerClient()
        self.alice = PeerNode("alice", "alice", self.tracker, "127.0.0.1", 0, 0.05)
        self.bob = PeerNode("bob", "bob", self.tracker, "127.0.0.1", 0, 0.05)
        await self.alice.start()
        await self.bob.start()

    async def asyncTearDown(self):
        await self.alice.close()
        await self.bob.close()

    async def test_discovery_connect_exchange_and_cleanup(self):
        peers = await self.alice.discover()
        self.assertEqual([peer["peer_id"] for peer in peers], ["bob"])
        await self.alice.connect_peer("bob")
        await self.alice.send("bob", {"type": "example", "text": "xin chao"})
        peer_id, message = await asyncio.wait_for(self.bob.incoming.get(), 2)
        self.assertEqual((peer_id, message["text"]), ("alice", "xin chao"))
        await self.bob.send("alice", {"type": "reply"})
        peer_id, message = await asyncio.wait_for(self.alice.incoming.get(), 2)
        self.assertEqual((peer_id, message["type"]), ("bob", "reply"))
        await self.alice.disconnect_peer("bob")
        for _ in range(100):
            if "alice" not in self.bob.connections:
                break
            await asyncio.sleep(0.01)
        self.assertNotIn("alice", self.bob.connections)
        await self.alice.close()
        self.assertEqual([peer["peer_id"] for peer in self.tracker.service.list_active_peers()], ["bob"])

    async def test_heartbeat_is_sent(self):
        await asyncio.sleep(0.13)
        self.assertGreaterEqual(self.tracker.heartbeats, 2)


if __name__ == "__main__":
    unittest.main()
