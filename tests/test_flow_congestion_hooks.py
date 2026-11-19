import os
import queue
import struct
import sys
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from streaprous_socket import streaprous_raw_connection as raw
from modules.transport_control.congestion_control import MSS, CUBIC_BETA
from modules.transport_control.flow_control import RCV_BUFFER
from modules.transport_control.tcp_state import create_tcp_state, drop_tcp_state


class FakeSocket:
    def __init__(self):
        self.sent = []

    def sendto(self, packet, address):
        self.sent.append((packet, address))
        return len(packet)


class ScriptedQueue:
    def __init__(self, packets):
        self.packets = list(packets)

    def get(self, timeout):
        if not self.packets:
            raise queue.Empty()
        item = self.packets.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def incoming(flags, seq, ack, window=5840, payload=b""):
    ip_header = bytes([0x45]) + bytes(19)
    tcp_header = struct.pack("!HHLLBBHHH", 40000, 9000, seq, ack, 5 << 4, flags, window, 0, 0)
    return (ip_header + tcp_header + payload, ("127.0.0.1", 0))


def ack_packet(ack, window=5840):
    return incoming(0x10, 101, ack, window)


def tcp_window(packet):
    return struct.unpack("!H", packet[34:36])[0]


def tcp_ack(packet):
    return struct.unpack("!L", packet[28:32])[0]


class HookTestBase(unittest.TestCase):

    def setUp(self):
        self.key = ("127.0.0.1", 40000)
        self.connection = raw.conn_struct(
            "127.0.0.1", 40000, "127.0.0.2", 9000,
            "ESTABLISHED", 2001, 101, 2001, time.time(),
            waiting=None, estimated_rtt=0.5, dev_rtt=0.25, rto=1.0,
        )
        with raw.connections_lock:
            raw.connections[self.key] = self.connection
        self.state = create_tcp_state(self.key)

    def tearDown(self):
        with raw.connections_lock:
            raw.connections.pop(self.key, None)
        drop_tcp_state(self.key)

    def send(self, packets, payload=b"abc"):
        sock = FakeSocket()
        with patch.object(raw, "get_packet_queue", return_value=ScriptedQueue(packets)):
            result = raw.run_conn_send(sock, self.connection, payload)
        return result, sock


class TestSendHooks(HookTestBase):

    def test_new_ack_grows_cwnd(self):
        result, _ = self.send([ack_packet(2004)])
        self.assertEqual(result, 3)
        self.assertEqual(self.state.cc.cwnd, 2 * MSS)

    def test_timeout_resets_cwnd(self):
        self.state.cc.cwnd = 10.0 * MSS
        result, sock = self.send([])
        self.assertEqual(result, -1)
        self.assertEqual(len(sock.sent), 5)
        self.assertEqual(self.state.cc.cwnd, MSS)
        self.assertEqual(self.state.cc.state, "slow_start")

    def test_timeout_then_ack(self):
        self.state.cc.cwnd = 10.0 * MSS
        result, _ = self.send([queue.Empty(), ack_packet(2004)])
        self.assertEqual(result, 3)
        self.assertAlmostEqual(self.state.cc.ssthresh, 10 * MSS * CUBIC_BETA)
        self.assertEqual(self.state.cc.cwnd, 2 * MSS)

    def test_triple_dup_ack_cuts_cwnd(self):
        self.state.cc.cwnd = 10.0 * MSS
        packets = [ack_packet(2001), ack_packet(2001), ack_packet(2001), ack_packet(2004)]
        result, _ = self.send(packets)
        self.assertEqual(result, 3)
        self.assertAlmostEqual(self.state.cc.ssthresh, 10 * MSS * CUBIC_BETA)
        self.assertGreaterEqual(self.state.cc.cwnd, self.state.cc.ssthresh)
        self.assertLess(self.state.cc.cwnd, 10 * MSS)

    def test_data_segment_carries_rwnd(self):
        _, sock = self.send([ack_packet(2004)])
        self.assertEqual(tcp_window(sock.sent[0][0]), RCV_BUFFER)

    def test_ack_updates_peer_rwnd(self):
        self.send([ack_packet(2004, window=1234)])
        self.assertEqual(self.state.peer_rwnd, 1234)


class TestRecvHooks(HookTestBase):

    def recv(self, packets):
        sock = FakeSocket()
        with patch.object(raw, "get_packet_queue", return_value=ScriptedQueue(packets)):
            data = raw.run_conn_recv(sock, self.connection, timeout=0.2)
        return data, sock

    def test_ack_advertises_rwnd_after_data(self):
        data, sock = self.recv([incoming(0x18, 101, 2001, payload=b"hello")])
        self.assertEqual(data, b"hello")
        self.assertEqual(tcp_window(sock.sent[0][0]), RCV_BUFFER - 5)
        self.assertEqual(self.state.rcv.rwnd, RCV_BUFFER)

    def test_out_of_order_data_not_buffered(self):
        packets = [incoming(0x18, 999, 2001, payload=b"bad"), incoming(0x18, 101, 2001, payload=b"ok")]
        data, sock = self.recv(packets)
        self.assertEqual(data, b"ok")
        self.assertEqual(tcp_window(sock.sent[0][0]), RCV_BUFFER)
        self.assertEqual(self.state.rcv.last_byte_rcvd, 2)

    def test_incoming_packet_updates_peer_rwnd(self):
        self.recv([incoming(0x18, 101, 2001, window=4321, payload=b"hi")])
        self.assertEqual(self.state.peer_rwnd, 4321)


class TestCloseConnection(HookTestBase):

    def test_fin_acks_client_seq(self):
        self.state.rcv.on_data(b"x" * 10)
        sock = FakeSocket()
        raw.close_connection(sock, self.connection)
        self.assertEqual(tcp_ack(sock.sent[0][0]), 101)


if __name__ == "__main__":
    unittest.main()
