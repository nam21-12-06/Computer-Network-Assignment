"""Route-level tests for the tracker application contract."""

import json
import unittest

from apps import tracker_app
from apps.tracker import TrackerService


class TrackerAppTests(unittest.TestCase):
    def setUp(self) -> None:
        tracker_app.tracker = TrackerService(peer_ttl_seconds=60)

    def test_registration_uses_observed_remote_ip(self) -> None:
        response, status = tracker_app.register_peer(
            {"x-remote-ip": "192.168.56.50"},
            json.dumps(
                {
                    "peer_id": "peer-a",
                    "user_id": "alice",
                    "ip": "192.168.56.99",
                    "listen_port": 9100,
                }
            ),
        )

        self.assertEqual(status, 201)
        self.assertEqual(response["peer"]["ip"], "192.168.56.50")

    def test_connect_peer_returns_a_404_for_unknown_target(self) -> None:
        tracker_app.register_peer(
            {"x-remote-ip": "192.168.56.50"},
            json.dumps({"peer_id": "peer-a", "user_id": "alice", "listen_port": 9100}),
        )

        response, status = tracker_app.connect_peer(
            {}, json.dumps({"peer_id": "peer-a", "target_peer_id": "missing"})
        )

        self.assertEqual(status, 404)
        self.assertEqual(response["error"], "peer was not registered")


if __name__ == "__main__":
    unittest.main()
