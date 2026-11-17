"""Tests for one outstanding TCP payload per connection."""

import queue
import struct
import time
import unittest
from unittest.mock import patch

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


class ScriptedQueue:
    def __init__(self, packets):
        self.packets = list(packets)
        self.timeouts = []

    def get(self, timeout):
        self.timeouts.append(timeout)
        item = self.packets.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


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
        packet_queue = ScriptedQueue([(incoming_ack(2004), ("127.0.0.1", 0))])
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            self.assertEqual(raw.run_conn_send(sock, self.connection, b"abc"), 3)

        session = raw.connections[self.key]
        self.assertIsNone(session.pending)
        self.assertEqual(session.server_seq, 2004)
        self.assertEqual(struct.unpack("!I", sock.sent[0][0][24:28])[0], 2001)
        self.assertEqual(packet_queue.timeouts, [1.0])

    def test_second_payload_waits_for_first_ack(self):
        with raw.connections_lock:
            raw.connections[self.key] = self.connection._replace(pending=(2001, b"abc"))
        sock = FakeSocket()
        self.assertEqual(raw.run_conn_send(sock, self.connection, b"next"), -1)
        self.assertEqual(len(sock.sent), 0)
        self.assertEqual(raw.connections[self.key].pending, (2001, b"abc"))

    def test_send_failure_does_not_set_pending_payload(self):
        self.assertEqual(raw.run_conn_send(FakeSocket(fail=True), self.connection, b"abc"), -1)

        session = raw.connections[self.key]
        self.assertEqual(session.server_seq, 2001)
        self.assertIsNone(session.pending)

    def receive_ack(self, ack_number):
        with raw.connections_lock:
            raw.connections[self.key] = self.connection._replace(
                server_seq=2004, pending=(2001, b"abc")
            )
        packet_queue = queue.Queue()
        packet_queue.put((incoming_ack(ack_number), ("127.0.0.1", 0)))
        packet_queue.put((incoming_data(), ("127.0.0.1", 0)))
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            raw.run_conn_recv(FakeSocket(), self.connection)
        return raw.connections[self.key]

    def test_expected_ack_clears_pending_payload(self):
        session = self.receive_ack(2004)
        self.assertIsNone(session.pending)
        self.assertEqual(session.server_seq, 2004)

    def test_wrong_ack_keeps_pending_payload(self):
        session = self.receive_ack(2003)
        self.assertEqual(session.pending, (2001, b"abc"))
        self.assertEqual(session.server_seq, 2004)

    def test_timeout_retransmits_same_payload_and_sequence(self):
        packet_queue = ScriptedQueue([
            queue.Empty(),
            queue.Empty(),
            (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        sock = FakeSocket()
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            self.assertEqual(raw.run_conn_send(sock, self.connection, b"abc"), 3)

        sequences = [struct.unpack("!I", packet[24:28])[0]
                     for packet, _ in sock.sent]
        self.assertEqual(sequences, [2001, 2001, 2001])
        self.assertEqual(packet_queue.timeouts, [1.0, 2.0, 4.0])
        self.assertIsNone(raw.connections[self.key].pending)

    def test_stops_after_three_retransmissions(self):
        packet_queue = ScriptedQueue([queue.Empty() for _ in range(4)])
        sock = FakeSocket()
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            self.assertEqual(raw.run_conn_send(sock, self.connection, b"abc"), -1)

        self.assertEqual(len(sock.sent), 4)
        self.assertEqual(packet_queue.timeouts, [1.0, 2.0, 4.0, 8.0])
        self.assertEqual(raw.connections[self.key].pending, (2001, b"abc"))

    def test_first_rtt_sample_sets_rto(self):
        packet_queue = ScriptedQueue([(incoming_ack(2004), ("127.0.0.1", 0))])
        with patch.object(raw, "get_packet_queue", return_value=packet_queue), \
             patch.object(raw.time, "monotonic", side_effect=[10.0, 12.0]):
            raw.run_conn_send(FakeSocket(), self.connection, b"abc")

        session = raw.connections[self.key]
        self.assertEqual(session.srtt, 2.0)
        self.assertEqual(session.rttvar, 1.0)
        self.assertEqual(session.rto, 6.0)

    def test_second_rtt_sample_smooths_rto(self):
        first_queue = ScriptedQueue([(incoming_ack(2004), ("127.0.0.1", 0))])
        second_queue = ScriptedQueue([(incoming_ack(2006), ("127.0.0.1", 0))])
        with patch.object(raw, "get_packet_queue", side_effect=[first_queue, second_queue]), \
             patch.object(raw.time, "monotonic", side_effect=[0.0, 2.0, 2.0, 6.0]):
            raw.run_conn_send(FakeSocket(), self.connection, b"abc")
            raw.run_conn_send(FakeSocket(), self.connection, b"de")

        session = raw.connections[self.key]
        self.assertEqual(session.srtt, 2.25)
        self.assertEqual(session.rttvar, 1.25)
        self.assertEqual(session.rto, 7.25)

    def test_retransmitted_data_does_not_update_rtt(self):
        packet_queue = ScriptedQueue([
            queue.Empty(),
            (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        with patch.object(raw, "get_packet_queue", return_value=packet_queue), \
             patch.object(raw.time, "monotonic", side_effect=[0.0, 2.0]):
            raw.run_conn_send(FakeSocket(), self.connection, b"abc")

        session = raw.connections[self.key]
        self.assertIsNone(session.srtt)
        self.assertIsNone(session.rttvar)
        self.assertEqual(session.rto, 2.0)

    def test_empty_payload_returns_zero_without_sending(self):
        sock = FakeSocket()
        self.assertEqual(raw.run_conn_send(sock, self.connection, b""), 0)
        self.assertEqual(sock.sent, [])
        self.assertIsNone(raw.connections[self.key].pending)

    def test_missing_connection_cannot_send(self):
        with raw.connections_lock:
            raw.connections.pop(self.key)
        sock = FakeSocket()
        self.assertEqual(raw.run_conn_send(sock, self.connection, b"abc"), -1)
        self.assertEqual(sock.sent, [])

    def test_single_byte_waits_for_next_ack_number(self):
        packet_queue = ScriptedQueue([(incoming_ack(2002), ("127.0.0.1", 0))])
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            result = raw.run_conn_send(FakeSocket(), self.connection, b"x")
        self.assertEqual(result, 1)
        self.assertEqual(raw.connections[self.key].server_seq, 2002)

    def test_binary_payload_length_sets_expected_ack(self):
        payload = b"\x00\xff\x00"
        packet_queue = ScriptedQueue([(incoming_ack(2004), ("127.0.0.1", 0))])
        sock = FakeSocket()
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            result = raw.run_conn_send(sock, self.connection, payload)
        self.assertEqual(result, 3)
        self.assertEqual(sock.sent[0][0][40:], payload)
        self.assertIsNone(raw.connections[self.key].pending)

    def test_wrong_ack_is_skipped_until_correct_ack_arrives(self):
        packet_queue = ScriptedQueue([
            (incoming_ack(2003), ("127.0.0.1", 0)),
            (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            result = raw.run_conn_send(FakeSocket(), self.connection, b"abc")
        self.assertEqual(result, 3)
        self.assertEqual(packet_queue.timeouts, [1.0, 1.0])
        self.assertIsNone(raw.connections[self.key].pending)

    def test_packet_without_ack_flag_is_skipped(self):
        packet = bytearray(incoming_ack(2004))
        packet[33] = 0  # TCP flags byte
        packet_queue = ScriptedQueue([
            (bytes(packet), ("127.0.0.1", 0)),
            (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            result = raw.run_conn_send(FakeSocket(), self.connection, b"abc")
        self.assertEqual(result, 3)
        self.assertEqual(packet_queue.timeouts, [1.0, 1.0])

    def test_short_tcp_packet_is_skipped(self):
        short_packet = bytes([0x45]) + bytes(19) + bytes(10)
        packet_queue = ScriptedQueue([
            (short_packet, ("127.0.0.1", 0)),
            (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            result = raw.run_conn_send(FakeSocket(), self.connection, b"abc")
        self.assertEqual(result, 3)
        self.assertEqual(packet_queue.timeouts, [1.0, 1.0])

    def test_one_timeout_then_ack(self):
        packet_queue = ScriptedQueue([
            queue.Empty(), (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        sock = FakeSocket()
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            result = raw.run_conn_send(sock, self.connection, b"abc")
        self.assertEqual(result, 3)
        self.assertEqual(len(sock.sent), 2)
        self.assertEqual(packet_queue.timeouts, [1.0, 2.0])

    def test_timeout_doubles_existing_rto(self):
        with raw.connections_lock:
            raw.connections[self.key] = self.connection._replace(rto=3.0)
        packet_queue = ScriptedQueue([
            queue.Empty(), (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            raw.run_conn_send(FakeSocket(), self.connection, b"abc")
        self.assertEqual(packet_queue.timeouts, [3.0, 6.0])
        self.assertEqual(raw.connections[self.key].rto, 6.0)

    def test_timeout_backoff_stops_at_sixty_seconds(self):
        with raw.connections_lock:
            raw.connections[self.key] = self.connection._replace(rto=40.0)
        packet_queue = ScriptedQueue([
            queue.Empty(), queue.Empty(),
            (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            raw.run_conn_send(FakeSocket(), self.connection, b"abc")
        self.assertEqual(packet_queue.timeouts, [40.0, 60.0, 60.0])
        self.assertEqual(raw.connections[self.key].rto, 60.0)

    def test_retransmission_keeps_identical_payload_bytes(self):
        packet_queue = ScriptedQueue([
            queue.Empty(), (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        sock = FakeSocket()
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            raw.run_conn_send(sock, self.connection, b"a\x00b")
        self.assertEqual([packet[40:] for packet, _ in sock.sent],
                         [b"a\x00b", b"a\x00b"])

    def test_retransmission_keeps_previous_rtt_estimate(self):
        with raw.connections_lock:
            raw.connections[self.key] = self.connection._replace(
                srtt=2.0, rttvar=1.0, rto=6.0,
            )
        packet_queue = ScriptedQueue([
            queue.Empty(), (incoming_ack(2004), ("127.0.0.1", 0)),
        ])
        with patch.object(raw, "get_packet_queue", return_value=packet_queue), \
             patch.object(raw.time, "monotonic", side_effect=[0.0]):
            raw.run_conn_send(FakeSocket(), self.connection, b"abc")
        session = raw.connections[self.key]
        self.assertEqual((session.srtt, session.rttvar, session.rto),
                         (2.0, 1.0, 12.0))

    def test_short_rtt_uses_one_second_minimum_rto(self):
        packet_queue = ScriptedQueue([(incoming_ack(2004), ("127.0.0.1", 0))])
        with patch.object(raw, "get_packet_queue", return_value=packet_queue), \
             patch.object(raw.time, "monotonic", side_effect=[0.0, 0.1]):
            raw.run_conn_send(FakeSocket(), self.connection, b"abc")
        session = raw.connections[self.key]
        self.assertAlmostEqual(session.srtt, 0.1)
        self.assertEqual(session.rto, 1.0)

    def test_long_rtt_uses_sixty_second_maximum_rto(self):
        packet_queue = ScriptedQueue([(incoming_ack(2004), ("127.0.0.1", 0))])
        with patch.object(raw, "get_packet_queue", return_value=packet_queue), \
             patch.object(raw.time, "monotonic", side_effect=[0.0, 100.0]):
            raw.run_conn_send(FakeSocket(), self.connection, b"abc")
        session = raw.connections[self.key]
        self.assertEqual((session.srtt, session.rttvar, session.rto),
                         (100.0, 50.0, 60.0))

    def test_equal_rtt_samples_reduce_variation(self):
        first = ScriptedQueue([(incoming_ack(2004), ("127.0.0.1", 0))])
        second = ScriptedQueue([(incoming_ack(2006), ("127.0.0.1", 0))])
        with patch.object(raw, "get_packet_queue", side_effect=[first, second]), \
             patch.object(raw.time, "monotonic", side_effect=[0.0, 2.0, 3.0, 5.0]):
            raw.run_conn_send(FakeSocket(), self.connection, b"abc")
            raw.run_conn_send(FakeSocket(), self.connection, b"de")
        session = raw.connections[self.key]
        self.assertEqual((session.srtt, session.rttvar, session.rto),
                         (2.0, 0.75, 5.0))

    def test_larger_second_rtt_sample_raises_rto_gradually(self):
        first = ScriptedQueue([(incoming_ack(2004), ("127.0.0.1", 0))])
        second = ScriptedQueue([(incoming_ack(2006), ("127.0.0.1", 0))])
        with patch.object(raw, "get_packet_queue", side_effect=[first, second]), \
             patch.object(raw.time, "monotonic", side_effect=[0.0, 2.0, 10.0, 20.0]):
            raw.run_conn_send(FakeSocket(), self.connection, b"abc")
            raw.run_conn_send(FakeSocket(), self.connection, b"de")
        session = raw.connections[self.key]
        self.assertEqual((session.srtt, session.rttvar, session.rto),
                         (3.0, 2.75, 14.0))

    def test_send_sequence_and_ack_wrap_at_32_bits(self):
        with raw.connections_lock:
            raw.connections[self.key] = self.connection._replace(server_seq=0xFFFFFFFE)
        packet_queue = ScriptedQueue([(incoming_ack(1), ("127.0.0.1", 0))])
        sock = FakeSocket()
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            result = raw.run_conn_send(sock, self.connection, b"abc")
        self.assertEqual(result, 3)
        self.assertEqual(struct.unpack("!I", sock.sent[0][0][24:28])[0],
                         0xFFFFFFFE)
        self.assertEqual(raw.connections[self.key].server_seq, 1)


if __name__ == "__main__":
    unittest.main()
