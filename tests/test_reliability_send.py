"""Tests for one outstanding TCP payload per connection."""

import struct
import time
import unittest

from streaprous_socket import streaprous_raw_connection as raw


class FakeSocket:
    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    def sendto(self, packet, address):
        if self.fail:
            raise OSError("send failed")
        self.sent.append((packet, address))
        return len(packet)


class PendingSendTests(unittest.TestCase):
    def setUp(self):
        self.key = ("127.0.0.1", 40000)
        self.connection = raw.conn_struct(
            "127.0.0.1", 40000, "127.0.0.2", 9000,
            "ESTABLISHED", 2001, 101, 2001, time.time(),
        )
        with raw.connections_lock:
            raw.connections[self.key] = self.connection

    def tearDown(self):
        with raw.connections_lock:
            raw.connections.pop(self.key, None)

    def test_sent_payload_is_kept_until_ack(self):
        sock = FakeSocket()
        self.assertEqual(raw.run_conn_send(sock, self.connection, b"abc"), 3)

        session = raw.connections[self.key]
        self.assertEqual(session.pending, (2001, b"abc"))
        self.assertEqual(session.server_seq, 2004)
        self.assertEqual(struct.unpack("!I", sock.sent[0][0][24:28])[0], 2001)

    def test_second_payload_waits_for_first_ack(self):
        sock = FakeSocket()
        raw.run_conn_send(sock, self.connection, b"abc")

        self.assertEqual(raw.run_conn_send(sock, self.connection, b"next"), -1)
        self.assertEqual(len(sock.sent), 1)
        self.assertEqual(raw.connections[self.key].pending, (2001, b"abc"))

    def test_send_failure_does_not_set_pending_payload(self):
        self.assertEqual(raw.run_conn_send(FakeSocket(fail=True), self.connection, b"abc"), -1)

        session = raw.connections[self.key]
        self.assertEqual(session.server_seq, 2001)
        self.assertIsNone(session.pending)


if __name__ == "__main__":
    unittest.main()
