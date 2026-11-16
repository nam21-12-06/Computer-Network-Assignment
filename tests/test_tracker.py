"""Unit tests for tracker, discovery, cleanup, and channel management."""

import unittest

from apps.tracker import TrackerError, TrackerService


class TrackerServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tracker = TrackerService(peer_ttl_seconds=60, clock=lambda: 101)
        self.tracker.register_peer("peer-a", "alice", "192.168.56.10", 9100, now=100)
        self.tracker.register_peer("peer-b", "bob", "192.168.56.11", 9101, now=100)

    def test_registering_an_existing_peer_refreshes_its_endpoint(self) -> None:
        peer = self.tracker.register_peer("peer-a", "alice", "192.168.56.12", 9200, now=105)

        self.assertEqual(peer["ip"], "192.168.56.12")
        self.assertEqual(peer["listen_port"], 9200)
        self.assertEqual(peer["registered_at"], 100)
        self.assertEqual(len(self.tracker.list_active_peers(now=105)), 2)

    def test_peer_discovery_excludes_the_requesting_peer(self) -> None:
        peers = self.tracker.list_active_peers(exclude_peer_id="peer-a", now=101)

        self.assertEqual([peer["peer_id"] for peer in peers], ["peer-b"])

    def test_expired_peer_is_not_discoverable(self) -> None:
        self.tracker.heartbeat("peer-b", now=150)
        expired = self.tracker.cleanup_expired_peers(now=161)

        self.assertEqual(expired, ["peer-a"])
        self.assertEqual([peer["peer_id"] for peer in self.tracker.list_active_peers(now=161)], ["peer-b"])

    def test_disconnect_removes_peer_from_active_list(self) -> None:
        disconnected = self.tracker.disconnect_peer("peer-a", now=110)

        self.assertEqual(disconnected["status"], "inactive")
        self.assertEqual([peer["peer_id"] for peer in self.tracker.list_active_peers(now=110)], ["peer-b"])

    def test_connect_peer_returns_only_the_target_endpoint(self) -> None:
        target = self.tracker.connect_peer("peer-a", "peer-b", now=101)

        self.assertEqual(
            target,
            {"peer_id": "peer-b", "user_id": "bob", "ip": "192.168.56.11", "listen_port": 9101},
        )

    def test_connecting_to_inactive_peer_is_rejected(self) -> None:
        self.tracker.disconnect_peer("peer-b", now=110)

        with self.assertRaises(TrackerError) as error:
            self.tracker.connect_peer("peer-a", "peer-b", now=111)

        self.assertEqual(error.exception.status_code, 409)

    def test_channel_membership_and_listing(self) -> None:
        channel = self.tracker.create_channel("peer-a", "networking", now=101)
        self.tracker.join_channel("peer-b", channel["channel_id"])

        channels = self.tracker.list_channels_for_peer("peer-b")
        members = self.tracker.list_channel_members(channel["channel_id"])

        self.assertEqual(channels[0]["channel_id"], channel["channel_id"])
        self.assertEqual([member["peer_id"] for member in members], ["peer-a", "peer-b"])

    def test_owner_leaving_transfers_owner_and_empty_channel_is_deleted(self) -> None:
        channel = self.tracker.create_channel("peer-a", "networking", now=101)
        self.tracker.join_channel("peer-b", channel["channel_id"])

        updated = self.tracker.leave_channel("peer-a", channel["channel_id"])
        deleted = self.tracker.leave_channel("peer-b", channel["channel_id"])

        self.assertEqual(updated["owner_id"], "peer-b")
        self.assertEqual(deleted, {"channel_id": channel["channel_id"], "deleted": True})


if __name__ == "__main__":
    unittest.main()
