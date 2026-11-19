#
# Integration tests for Transport Layer (Role A: Nam)
# CO3093/CO3094 Computer Networks - HCMUT
#

import os
import sys
import unittest
import socket
import struct
import threading
import time
import queue

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streaprous_socket as transport
from streaprous_socket import (
    conn_struct,
    ConnectionInfo,
    transport_send,
    transport_recv,
    transport_close,
    RawTransportSocket,
    build_tcp_packet,
    build_ip_header,
    close_connection,
    close_all_connections,
)
from streaprous_socket.streaprous_raw_connection import (
    connections,
    connections_lock,
    tcp_states,
    tcp_states_lock,
    get_tcp_state,
    create_tcp_state,
    drop_tcp_state,
    connect,
)
from streaprous_socket.streaprous_packet_dispatcher import (
    get_packet_queue,
    remove_packet_queue,
)


class MockSocket:
    """Mock socket capturing outgoing raw packets for deterministic testing."""
    def __init__(self):
        self.sent_packets = []

    def sendto(self, data: bytes, addr) -> int:
        self.sent_packets.append((data, addr))
        return len(data)

    def settimeout(self, timeout: float):
        pass

    def recvfrom(self, bufsize: int):
        time.sleep(0.1)
        raise socket.timeout()

    def close(self):
        pass


def make_raw_tcp_segment(src_ip, dst_ip, src_port, dst_port, seq, ack_seq, flags, window=5840, payload=b""):
    offset_res = (5 << 4)
    tcp_hdr = struct.pack('!HHLLBBHHH', src_port, dst_port, seq, ack_seq, offset_res, flags, window, 0, 0)
    ip_hdr = build_ip_header(src_ip, dst_ip, socket.IPPROTO_TCP, len(tcp_hdr) + len(payload))
    return ip_hdr + tcp_hdr + payload


class TestTransportInterface(unittest.TestCase):
    """Test unified transport interface and type definitions."""

    def test_connection_info_alias(self):
        # Verification of page 29 specification: ConnectionInfo = conn_struct
        self.assertIs(ConnectionInfo, conn_struct)

    def test_exported_symbols(self):
        # Ensure streaprous_socket package exposes required API
        expected = [
            "conn_struct", "ConnectionInfo", "create_raw_socket",
            "accept", "connect", "run_conn_send", "run_conn_recv",
            "close_connection", "close_all_connections",
            "transport_send", "transport_recv", "transport_close",
            "RawTransportSocket",
        ]
        for name in expected:
            self.assertTrue(hasattr(transport, name), f"Missing exported symbol: {name}")


class TestRawTransportSocketWrapper(unittest.TestCase):
    """Test OOP RawTransportSocket wrapper methods."""

    def setUp(self):
        self.client_ip = "192.168.1.55"
        self.client_port = 50055
        self.server_ip = "192.168.1.1"
        self.server_port = 9000
        self.client_key = (self.client_ip, self.client_port)

        self.mock_sock = MockSocket()
        self.wrapper = RawTransportSocket(raw_sock=self.mock_sock)
        self.conn = conn_struct(
            client_ip=self.client_ip,
            client_port=self.client_port,
            server_ip=self.server_ip,
            server_port=self.server_port,
            state="ESTABLISHED",
            server_seq=2000,
            client_seq=1000,
            expected_final_ack=2001,
            last_activity=time.time(),
        )
        with connections_lock:
            connections[self.client_key] = self.conn
        create_tcp_state(self.client_key, rcv_nxt=1000)

    def tearDown(self):
        with connections_lock:
            connections.pop(self.client_key, None)
        drop_tcp_state(self.client_key)
        remove_packet_queue(self.client_key)

    def test_wrapper_close(self):
        self.wrapper.conn_info = self.conn
        self.wrapper.close()
        self.assertIsNone(self.wrapper.conn_info)
        self.assertNotIn(self.client_key, connections)
        self.assertIsNone(get_tcp_state(self.client_key))

    def test_wrapper_send_and_recv_delegation(self):
        self.wrapper.conn_info = self.conn
        # Non-blocking receive with timeout should return empty bytes when queue empty
        data = self.wrapper.recv(timeout=0.05)
        self.assertEqual(data, b"")

        # Functional transport_close
        transport_close(self.mock_sock, self.conn)
        self.assertNotIn(self.client_key, connections)


class TestActiveConnectHandshake(unittest.TestCase):
    """Test client-side active 3-way handshake connect()."""

    def setUp(self):
        self.src_ip = "192.168.1.100"
        self.src_port = 45000
        self.dst_ip = "192.168.1.200"
        self.dst_port = 8080
        self.client_key = (self.dst_ip, self.dst_port)
        self.mock_sock = MockSocket()

    def tearDown(self):
        with connections_lock:
            connections.pop(self.client_key, None)
        drop_tcp_state(self.client_key)
        remove_packet_queue(self.client_key)

    def test_connect_success(self):
        q = get_packet_queue(self.client_key)

        def peer_respond():
            # Wait for SYN packet from connect()
            while not self.mock_sock.sent_packets:
                time.sleep(0.01)
            # Push SYN-ACK response
            syn_ack = make_raw_tcp_segment(
                src_ip=self.dst_ip, dst_ip=self.src_ip,
                src_port=self.dst_port, dst_port=self.src_port,
                seq=5000, ack_seq=1001, flags=0x12, window=16384
            )
            q.put((syn_ack, (self.dst_ip, self.dst_port)))

        t = threading.Thread(target=peer_respond, daemon=True)
        t.start()

        conn_info = connect(self.mock_sock, self.src_ip, self.src_port, self.dst_ip, self.dst_port)

        self.assertEqual(conn_info.state, "ESTABLISHED")
        self.assertEqual(conn_info.server_seq, 1001)
        self.assertEqual(conn_info.client_seq, 5001)

        # SYN + final ACK sent
        self.assertEqual(len(self.mock_sock.sent_packets), 2)
        state = get_tcp_state(self.client_key)
        self.assertIsNotNone(state)
        self.assertEqual(state.peer_rwnd, 16384)


class TestConcurrencyAndThreadSafety(unittest.TestCase):
    """Test thread-safety and concurrency controls."""

    def test_tcp_control_state_lock(self):
        state = create_tcp_state(("127.0.0.1", 12345))
        self.assertTrue(hasattr(state, "lock"))
        self.assertIsInstance(state.lock, type(threading.Lock()))

        # Verify lock can be acquired and released across threads
        acquired = []

        def worker():
            with state.lock:
                acquired.append(True)
                time.sleep(0.02)

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(len(acquired), 2)
        drop_tcp_state(("127.0.0.1", 12345))


if __name__ == "__main__":
    unittest.main()
