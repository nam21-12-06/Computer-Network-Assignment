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


if __name__ == "__main__":
    unittest.main()
