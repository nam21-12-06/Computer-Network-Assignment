"""Kiem tra TrackerClient qua HTTP va cac route StreapRous that tren Linux."""

import asyncio
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from apps import tracker_app
from apps.peer import PeerNode, TrackerClient
from apps.tracker import TrackerService
from daemon.httpadapter import HttpAdapter
from daemon.request import Request


class TrackerHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        size = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(size).decode("utf-8")
        request = Request()
        request.prepare(f"POST {self.path} HTTP/1.1\r\nHost: localhost\r\n\r\n{body}", tracker_app.app.routes)
        adapter = HttpAdapter("127.0.0.1", self.server.server_port, None, self.client_address, {})
        response = adapter._run_route(request)
        self.wfile.write(response)

    def log_message(self, format, *args):
        pass


class TrackerHttpIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_peers_register_discover_connect_and_disconnect(self):
        tracker_app.tracker = TrackerService()
        server = ThreadingHTTPServer(("127.0.0.1", 0), TrackerHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = TrackerClient(f"http://127.0.0.1:{server.server_port}")
        alice = PeerNode("alice", "alice", client, "127.0.0.1", 0)
        bob = PeerNode("bob", "bob", client, "127.0.0.1", 0)
        try:
            await alice.start()
            await bob.start()
            self.assertEqual([peer["peer_id"] for peer in await alice.discover()], ["bob"])
            await alice.connect_peer("bob")
            await alice.send("bob", {"type": "example", "value": 42})
            sender, frame = await asyncio.wait_for(bob.incoming.get(), 2)
            self.assertEqual((sender, frame["value"]), ("alice", 42))
            await alice.close()
            self.assertEqual([peer["peer_id"] for peer in await bob.discover()], [])
        finally:
            await alice.close()
            await bob.close()
            await asyncio.to_thread(server.shutdown)
            server.server_close()
            thread.join(timeout=2)
