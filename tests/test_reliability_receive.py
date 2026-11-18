"""Receive-side sequence checks without a real raw socket."""

import queue
import struct
import time
import unittest
from unittest.mock import patch

from streaprous_socket import streaprous_raw_connection as raw


CLIENT_IP = "127.0.0.1"
SERVER_IP = "127.0.0.2"
CLIENT_PORT = 40000
SERVER_PORT = 9000


class FakeSocket:
    def __init__(self):
        self.sent = []

    def sendto(self, packet, address):
        self.sent.append((packet, address))
        return len(packet)


def incoming_data(seq, payload):
    ip_header = bytes([0x45]) + bytes(19)
    tcp_header = struct.pack(
        "!HHLLBBHHH", CLIENT_PORT, SERVER_PORT, seq, 0,
        5 << 4, 0x18, 5840, 0, 0,
    )
    return ip_header + tcp_header + payload


def incoming_ack(ack_number):
    ip_header = bytes([0x45]) + bytes(19)
    tcp_header = struct.pack(
        "!HHLLBBHHH", CLIENT_PORT, SERVER_PORT, 101, ack_number,
        5 << 4, 0x10, 5840, 0, 0,
    )
    return ip_header + tcp_header


def incoming_segment(seq, payload=b"", flags=0x18,
                     src_port=CLIENT_PORT, dest_port=SERVER_PORT, options=b""):
    """Build one test segment; options must have a length divisible by four."""
    ip_header = bytes([0x45]) + bytes(19)
    tcp_header = struct.pack(
        "!HHLLBBHHH", src_port, dest_port, seq, 0,
        (5 + len(options) // 4) << 4, flags, 5840, 0, 0,
    )
    return ip_header + tcp_header + options + payload


class ReceiveSequenceTests(unittest.TestCase):
    def setUp(self):
        self.key = (CLIENT_IP, CLIENT_PORT)
        self.connection = raw.conn_struct(
            CLIENT_IP, CLIENT_PORT, SERVER_IP, SERVER_PORT,
            "ESTABLISHED", 2001, 101, 2001, time.time(),
        )
        with raw.connections_lock:
            raw.connections[self.key] = self.connection

    def tearDown(self):
        with raw.connections_lock:
            raw.connections.pop(self.key, None)

    def receive(self, segments):
        packet_queue = queue.Queue()
        for seq, payload in segments:
            packet_queue.put((incoming_data(seq, payload), (CLIENT_IP, 0)))
        sock = FakeSocket()
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            data = raw.run_conn_recv(sock, self.connection)
        ack_numbers = [struct.unpack("!I", packet[28:32])[0]
                       for packet, _ in sock.sent]
        return data, ack_numbers

    def receive_packets(self, packets):
        packet_queue = queue.Queue()
        for packet in packets:
            packet_queue.put((packet, (CLIENT_IP, 0)))
        sock = FakeSocket()
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            data = raw.run_conn_recv(sock, self.connection)
        acks = [struct.unpack("!I", packet[28:32])[0]
                for packet, _ in sock.sent]
        return data, acks

    def test_in_order_data_advances_ack(self):
        data, acks = self.receive([(101, b"abc")])
        self.assertEqual(data, b"abc")
        self.assertEqual(acks, [104])
        self.assertEqual(raw.connections[self.key].client_seq, 104)

    def test_duplicate_data_is_not_delivered(self):
        data, acks = self.receive([(98, b"old"), (101, b"new")])
        self.assertEqual(data, b"new")
        self.assertEqual(acks, [101, 104])
        self.assertEqual(raw.connections[self.key].client_seq, 104)

    def test_out_of_order_data_is_not_delivered(self):
        data, acks = self.receive([(105, b"later"), (101, b"new")])
        self.assertEqual(data, b"new")
        self.assertEqual(acks, [101, 104])
        self.assertEqual(raw.connections[self.key].client_seq, 104)

    def test_old_ack_does_not_move_server_sequence(self):
        packet_queue = queue.Queue()
        packet_queue.put((incoming_ack(1999), (CLIENT_IP, 0)))
        packet_queue.put((incoming_data(101, b"ok"), (CLIENT_IP, 0)))
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            data = raw.run_conn_recv(FakeSocket(), self.connection)
        self.assertEqual(data, b"ok")
        self.assertEqual(raw.connections[self.key].server_seq, 2001)

    def test_future_ack_does_not_move_server_sequence(self):
        packet_queue = queue.Queue()
        packet_queue.put((incoming_ack(3000), (CLIENT_IP, 0)))
        packet_queue.put((incoming_data(101, b"ok"), (CLIENT_IP, 0)))
        with patch.object(raw, "get_packet_queue", return_value=packet_queue):
            data = raw.run_conn_recv(FakeSocket(), self.connection)
        self.assertEqual(data, b"ok")
        self.assertEqual(raw.connections[self.key].server_seq, 2001)

    def test_one_byte_advances_sequence_by_one(self):
        data, acks = self.receive([(101, b"x")])
        self.assertEqual((data, acks), (b"x", [102]))

    def test_binary_payload_counts_all_bytes(self):
        payload = b"\x00\xff\x00"
        data, acks = self.receive([(101, payload)])
        self.assertEqual((data, acks), (payload, [104]))

    def test_sequence_wraps_after_ffffffff(self):
        with raw.connections_lock:
            raw.connections[self.key] = self.connection._replace(client_seq=0xFFFFFFFE)
        data, acks = self.receive([(0xFFFFFFFE, b"abcd")])
        self.assertEqual((data, acks), (b"abcd", [2]))
        self.assertEqual(raw.connections[self.key].client_seq, 2)

    def test_repeated_packet_after_delivery_is_not_delivered_twice(self):
        self.receive([(101, b"abc")])
        data, acks = self.receive([(101, b"abc"), (104, b"d")])
        self.assertEqual((data, acks), (b"d", [104, 105]))

    def test_packet_entirely_before_expected_sequence_is_dropped(self):
        data, acks = self.receive([(90, b"old"), (101, b"ok")])
        self.assertEqual((data, acks), (b"ok", [101, 103]))

    def test_partially_overlapping_packet_is_dropped(self):
        data, acks = self.receive([(99, b"abcd"), (101, b"ok")])
        self.assertEqual((data, acks), (b"ok", [101, 103]))

    def test_one_byte_gap_is_not_accepted(self):
        data, acks = self.receive([(102, b"late"), (101, b"a")])
        self.assertEqual((data, acks), (b"a", [101, 102]))

    def test_larger_gap_is_not_accepted(self):
        data, acks = self.receive([(1000, b"late"), (101, b"a")])
        self.assertEqual((data, acks), (b"a", [101, 102]))

    def test_multiple_wrong_sequences_repeat_current_ack(self):
        data, acks = self.receive([(98, b"old"), (107, b"late"), (101, b"ok")])
        self.assertEqual((data, acks), (b"ok", [101, 101, 103]))

    def test_data_without_psh_is_delivered(self):
        data, acks = self.receive_packets([
            incoming_segment(101, b"ok", flags=0x10),
        ])
        self.assertEqual((data, acks), (b"ok", [103]))

    def test_tcp_options_are_skipped_before_payload(self):
        data, acks = self.receive_packets([
            incoming_segment(101, b"ok", options=b"\x01\x01\x01\x01"),
        ])
        self.assertEqual((data, acks), (b"ok", [103]))

    def test_wrong_source_port_is_ignored(self):
        data, acks = self.receive_packets([
            incoming_segment(101, b"bad", src_port=40001),
            incoming_data(101, b"ok"),
        ])
        self.assertEqual((data, acks), (b"ok", [103]))

    def test_wrong_destination_port_is_ignored(self):
        data, acks = self.receive_packets([
            incoming_segment(101, b"bad", dest_port=9001),
            incoming_data(101, b"ok"),
        ])
        self.assertEqual((data, acks), (b"ok", [103]))

    def test_short_tcp_header_is_ignored(self):
        data, acks = self.receive_packets([
            bytes([0x45]) + bytes(19) + bytes(10),
            incoming_data(101, b"ok"),
        ])
        self.assertEqual((data, acks), (b"ok", [103]))

    def test_empty_data_segment_does_not_advance_sequence(self):
        data, acks = self.receive_packets([
            incoming_segment(101), incoming_data(101, b"ok"),
        ])
        self.assertEqual((data, acks), (b"ok", [103]))

    def test_ack_without_pending_payload_is_ignored(self):
        data, acks = self.receive_packets([
            incoming_ack(2001), incoming_data(101, b"ok"),
        ])
        self.assertEqual((data, acks), (b"ok", [103]))
        self.assertEqual(raw.connections[self.key].server_seq, 2001)

    def test_wrapped_expected_ack_clears_pending(self):
        with raw.connections_lock:
            raw.connections[self.key] = self.connection._replace(
                server_seq=1, pending=(0xFFFFFFFE, b"abc"),
            )
        self.receive_packets([incoming_ack(1), incoming_data(101, b"ok")])
        self.assertIsNone(raw.connections[self.key].pending)

    def test_wrong_ack_does_not_clear_wrapped_pending(self):
        with raw.connections_lock:
            raw.connections[self.key] = self.connection._replace(
                server_seq=1, pending=(0xFFFFFFFE, b"abc"),
            )
        self.receive_packets([incoming_ack(0), incoming_data(101, b"ok")])
        self.assertEqual(raw.connections[self.key].pending, (0xFFFFFFFE, b"abc"))


if __name__ == "__main__":
    unittest.main()
