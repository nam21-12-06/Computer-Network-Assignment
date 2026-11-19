"""Tests for reliability send mechanism and RTO calculation.

Matches modules/reliability.py implementation by Thành (feature/reliability).
"""

import os
import queue
import struct
import sys
import time
import unittest
from unittest.mock import patch

from transport_support import ScriptedQueue, scripted_transport

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from streaprous_socket import streaprous_raw_connection as raw
from modules.transport_control import reliability


class FakeSocket:
    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    def sendto(self, packet, address):
        if self.fail:
            raise OSError("send failed")
        self.sent.append((packet, address))
        return len(packet)


def incoming_ack(ack_number):
    ip_header = bytes([0x45]) + bytes(19)
    tcp_header = struct.pack(
        "!HHLLBBHHH", 40000, 9000, 101, ack_number,
        5 << 4, 0x10, 5840, 0, 0,
    )
    return ip_header + tcp_header


def incoming_data():
    ip_header = bytes([0x45]) + bytes(19)
    tcp_header = struct.pack(
        "!HHLLBBHHH", 40000, 9000, 101, 0,
        5 << 4, 0x18, 5840, 0, 0,
    )
    return ip_header + tcp_header + b"ok"


class PendingSendTests(unittest.TestCase):
    def setUp(self):
        self.key = ("127.0.0.1", 40000)
        raw.drop_tcp_state(self.key)
        self.connection = raw.conn_struct(
            "127.0.0.1", 40000, "127.0.0.2", 9000,
            "ESTABLISHED", 2001, 101, 2001, time.time(),
            waiting=None, estimated_rtt=0.5, dev_rtt=0.25, rto=1.0,
        )
        with raw.connections_lock:
            raw.connections[self.key] = self.connection

    def tearDown(self):
        with raw.connections_lock:
            raw.connections.pop(self.key, None)
        raw.drop_tcp_state(self.key)
        raw.remove_packet_queue(self.key)

    def test_sent_payload_is_kept_until_ack(self):
        sock = FakeSocket()
        packet_queue = ScriptedQueue([(incoming_ack(2004), ("127.0.0.1", 0))])
        with scripted_transport(raw, self.key, packet_queue):
            self.assertEqual(raw.run_conn_send(sock, self.connection, b"abc"), 3)

        session = raw.connections[self.key]
        self.assertIsNone(session.waiting)
        self.assertEqual(session.server_seq, 2004)
        self.assertEqual(struct.unpack("!I", sock.sent[0][0][24:28])[0], 2001)

    def test_second_payload_waits_for_first_ack(self):
        with raw.connections_lock:
            raw.connections[self.key] = self.connection._replace(waiting=(2001, b"abc"))
        sock = FakeSocket()
        self.assertEqual(raw.run_conn_send(sock, self.connection, b"next"), -1)
        self.assertEqual(len(sock.sent), 0)
        self.assertEqual(raw.connections[self.key].waiting, (2001, b"abc"))

    def test_send_failure_does_not_set_pending_payload(self):
        self.assertEqual(raw.run_conn_send(FakeSocket(fail=True), self.connection, b"abc"), -1)

        session = raw.connections[self.key]
        self.assertEqual(session.server_seq, 2001)
        self.assertIsNone(session.waiting)

    def receive_ack(self, ack_number):
        with raw.connections_lock:
            raw.connections[self.key] = self.connection._replace(
                server_seq=2004, waiting=(2001, b"abc")
            )
        packet_queue = queue.Queue()
        packet_queue.put((incoming_ack(ack_number), ("127.0.0.1", 0)))
        packet_queue.put((incoming_data(), ("127.0.0.1", 0)))
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            raw.run_conn_recv(FakeSocket(), self.connection)
        return raw.connections[self.key]

    def test_expected_ack_clears_pending_payload(self):
        session = self.receive_ack(2004)
        self.assertIsNone(session.waiting)

    def test_wrong_ack_keeps_pending_payload(self):
        session = self.receive_ack(2003)
        self.assertEqual(session.waiting, (2001, b"abc"))
        self.assertEqual(session.server_seq, 2004)

    def test_timeout_retransmits_same_payload_and_sequence(self):
        packet_queue = ScriptedQueue([
            queue.Empty(),
            queue.Empty(),
            (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        sock = FakeSocket()
        with scripted_transport(raw, self.key, packet_queue):
            self.assertEqual(raw.run_conn_send(sock, self.connection, b"abc"), 3)

        sequences = [struct.unpack("!I", packet[24:28])[0] for packet, _ in sock.sent]
        self.assertEqual(sequences, [2001, 2001, 2001])
        self.assertIsNone(raw.connections[self.key].waiting)

    def test_stops_after_five_attempts(self):
        packet_queue = ScriptedQueue([queue.Empty() for _ in range(5)])
        sock = FakeSocket()
        with scripted_transport(raw, self.key, packet_queue):
            self.assertEqual(raw.run_conn_send(sock, self.connection, b"abc"), -1)

        self.assertEqual(len(sock.sent), 5)

    def test_first_rtt_sample_updates_timeout(self):
        packet_queue = ScriptedQueue([(incoming_ack(2004), ("127.0.0.1", 0))])
        packet_queue.rtt = 1.0
        with scripted_transport(raw, self.key, packet_queue):
            raw.run_conn_send(FakeSocket(), self.connection, b"abc")

        session = raw.connections[self.key]
        self.assertAlmostEqual(session.estimated_rtt, 0.5625)
        self.assertAlmostEqual(session.dev_rtt, 0.296875)
        self.assertAlmostEqual(session.rto, 1.75)

    def test_retransmitted_data_does_not_update_rtt(self):
        packet_queue = ScriptedQueue([
            queue.Empty(),
            (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        with scripted_transport(raw, self.key, packet_queue):
            raw.run_conn_send(FakeSocket(), self.connection, b"abc")

        session = raw.connections[self.key]
        # Karn's algorithm: sample is not taken on retransmission
        self.assertEqual(session.estimated_rtt, 0.5)
        self.assertEqual(session.dev_rtt, 0.25)
        self.assertEqual(session.rto, 1.0)

    def test_empty_payload_returns_zero_without_sending(self):
        sock = FakeSocket()
        self.assertEqual(raw.run_conn_send(sock, self.connection, b""), 0)
        self.assertEqual(sock.sent, [])
        self.assertIsNone(raw.connections[self.key].waiting)

    def test_missing_connection_cannot_send(self):
        with raw.connections_lock:
            raw.connections.pop(self.key)
        sock = FakeSocket()
        self.assertEqual(raw.run_conn_send(sock, self.connection, b"abc"), -1)
        self.assertEqual(sock.sent, [])

    def test_single_byte_waits_for_next_ack_number(self):
        packet_queue = ScriptedQueue([(incoming_ack(2002), ("127.0.0.1", 0))])
        with scripted_transport(raw, self.key, packet_queue):
            result = raw.run_conn_send(FakeSocket(), self.connection, b"x")
        self.assertEqual(result, 1)
        self.assertEqual(raw.connections[self.key].server_seq, 2002)

    def test_binary_payload_length_sets_expected_ack(self):
        payload = b"\x00\xff\x00"
        packet_queue = ScriptedQueue([(incoming_ack(2004), ("127.0.0.1", 0))])
        sock = FakeSocket()
        with scripted_transport(raw, self.key, packet_queue):
            result = raw.run_conn_send(sock, self.connection, payload)
        self.assertEqual(result, 3)
        self.assertEqual(sock.sent[0][0][40:], payload)
        self.assertIsNone(raw.connections[self.key].waiting)

    def test_wrong_ack_is_skipped_until_correct_ack_arrives(self):
        packet_queue = ScriptedQueue([
            (incoming_ack(2003), ("127.0.0.1", 0)),
            (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        with scripted_transport(raw, self.key, packet_queue):
            result = raw.run_conn_send(FakeSocket(), self.connection, b"abc")
        self.assertEqual(result, 3)
        self.assertIsNone(raw.connections[self.key].waiting)

    def test_retransmission_keeps_identical_payload_bytes(self):
        packet_queue = ScriptedQueue([
            queue.Empty(), (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        sock = FakeSocket()
        with scripted_transport(raw, self.key, packet_queue):
            raw.run_conn_send(sock, self.connection, b"a\x00b")
        self.assertEqual([packet[40:] for packet, _ in sock.sent],
                         [b"a\x00b", b"a\x00b"])

    def test_send_sequence_and_ack_wrap_at_32_bits(self):
        with raw.connections_lock:
            raw.connections[self.key] = self.connection._replace(server_seq=0xFFFFFFFE)
        packet_queue = ScriptedQueue([(incoming_ack(1), ("127.0.0.1", 0))])
        sock = FakeSocket()
        with scripted_transport(raw, self.key, packet_queue):
            result = raw.run_conn_send(sock, self.connection, b"abc")
        self.assertEqual(result, 3)
        self.assertEqual(struct.unpack("!I", sock.sent[0][0][24:28])[0], 0xFFFFFFFE)
        self.assertEqual(raw.connections[self.key].server_seq, 1)


class ReliabilityModuleUnitTests(unittest.TestCase):
    """Direct unit tests for functions in modules/reliability.py."""

    def test_check_sequence_in_order(self):
        in_order, next_ack = reliability.check_sequence(100, 100, b"hello")
        self.assertTrue(in_order)
        self.assertEqual(next_ack, 105)

    def test_check_sequence_out_of_order(self):
        in_order, next_ack = reliability.check_sequence(100, 105, b"hello")
        self.assertFalse(in_order)
        self.assertEqual(next_ack, 100)

    def test_check_ack_matching(self):
        waiting = (1000, b"test")
        self.assertTrue(reliability.check_ack(waiting, 1004))
        self.assertFalse(reliability.check_ack(waiting, 1000))

    def test_check_ack_none_waiting(self):
        self.assertFalse(reliability.check_ack(None, 1004))

    def test_update_timeout_formula(self):
        session = raw.conn_struct(
            "1.1.1.1", 1, "2.2.2.2", 2, "ESTABLISHED", 0, 0, 0, 0,
            waiting=None, estimated_rtt=0.5, dev_rtt=0.25, rto=1.0,
        )
        updated = reliability.update_timeout(session, sample_rtt=1.0)
        # alpha=0.125, beta=0.25
        # estimated = 0.875 * 0.5 + 0.125 * 1.0 = 0.5625
        # dev = 0.75 * 0.25 + 0.25 * abs(1.0 - 0.5625) = 0.296875
        # rto = 0.5625 + 4 * 0.296875 = 1.75
        self.assertAlmostEqual(updated.estimated_rtt, 0.5625)
        self.assertAlmostEqual(updated.dev_rtt, 0.296875)
        self.assertAlmostEqual(updated.rto, 1.75)


if __name__ == "__main__":
    unittest.main()
