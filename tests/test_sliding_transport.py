import queue
import socket
import struct
import threading
import time
import unittest

from streaprous_socket import streaprous_raw_connection as raw
from streaprous_socket import streaprous_packet_dispatcher as dispatcher
from modules.transport_control.congestion_control import MSS
from modules.transport_control.flow_control import MAX_WINDOW, ReceiveBuffer
from transport_support import ScriptedQueue, scripted_transport


def incoming(ack=2001, seq=101, payload=b'', window=MAX_WINDOW, flags=16):
    tcp = struct.pack('!HHLLBBHHH', 40000, 9000, seq, ack, 5 << 4,
                      flags, window, 0, 0) + payload
    return raw.build_ip_header('127.0.0.1', '127.0.0.2', 6, len(tcp)) + tcp


def fields(packet):
    ihl = (packet[0] & 15) * 4
    header = struct.unpack('!HHLLBBHHH', packet[ihl:ihl + 20])
    return header, packet[ihl + (header[4] >> 4) * 4:]


class FakeSocket:
    def __init__(self, respond=None):
        self.sent = []
        self.respond = respond

    def sendto(self, packet, address):
        self.sent.append(packet)
        if self.respond:
            self.respond(packet)
        return len(packet)


class SlidingTransportTests(unittest.TestCase):
    def setUp(self):
        self.key = ('127.0.0.1', 40000)
        raw.close_all_connections()
        self.connection = raw.conn_struct(
            '127.0.0.1', 40000, '127.0.0.2', 9000,
            'ESTABLISHED', 2001, 101, 2001, time.time())
        raw.connections[self.key] = self.connection
        self.state = raw.create_tcp_state(self.key)

    def tearDown(self):
        raw.close_all_connections()

    def deliver(self, sock, **kwargs):
        raw.process_packet(sock, self.key, incoming(**kwargs))

    def test_cwnd_allows_two_segments_before_cumulative_ack(self):
        self.state.cc.cwnd = 2.0 * MSS
        inflight = []

        def respond(packet):
            header, payload = fields(packet)
            inflight.append(self.state.sent - self.state.acked)
            self.assertLessEqual(inflight[-1], min(self.state.cc.cwnd, self.state.peer_rwnd))
            if len(sock.sent) % 2 == 0:
                self.deliver(sock, ack=header[2] + len(payload))

        sock = FakeSocket(respond)
        payload = b'ab' * (2 * MSS)
        self.assertEqual(raw.run_conn_send(sock, self.connection, payload), len(payload))
        self.assertEqual(inflight[:2], [MSS, 2 * MSS])
        self.assertEqual(b''.join(fields(p)[1] for p in sock.sent), payload)
        self.assertTrue(all(len(fields(p)[1]) <= MSS for p in sock.sent))

    def test_peer_window_limits_each_send(self):
        self.state.peer_rwnd = 100

        def respond(packet):
            header, payload = fields(packet)
            self.assertLessEqual(self.state.sent - self.state.acked, 100)
            self.deliver(sock, ack=header[2] + len(payload), window=100)

        sock = FakeSocket(respond)
        self.assertEqual(raw.run_conn_send(sock, self.connection, b'x' * 250), 250)
        self.assertEqual([len(fields(p)[1]) for p in sock.sent], [100, 100, 50])

    def test_new_ack_opens_capacity_for_two_segments(self):
        inflight = []

        def respond(packet):
            header, payload = fields(packet)
            inflight.append(self.state.sent - self.state.acked)
            if len(sock.sent) in (1, 3):
                self.deliver(sock, ack=header[2] + len(payload))

        sock = FakeSocket(respond)
        self.assertEqual(raw.run_conn_send(sock, self.connection, b'x' * (3 * MSS)), 3 * MSS)
        self.assertEqual(inflight, [MSS, MSS, 2 * MSS])

    def test_consecutive_sends_use_updated_sequence(self):
        def respond(packet):
            header, payload = fields(packet)
            self.deliver(sock, ack=header[2] + len(payload))

        sock = FakeSocket(respond)
        self.assertEqual(raw.run_conn_send(sock, self.connection, b'abc'), 3)
        self.assertEqual(raw.run_conn_send(sock, self.connection, b'de'), 2)
        self.assertEqual([fields(p)[0][2] for p in sock.sent], [2001, 2004])

    def test_partial_ack_retransmits_only_unacknowledged_suffix(self):
        packets = ScriptedQueue([(incoming(ack=2003, window=2), None),
                                 queue.Empty(), (incoming(ack=2005), None)])
        sock = FakeSocket()
        with scripted_transport(raw, self.key, packets):
            self.assertEqual(raw.run_conn_send(sock, self.connection, b'abcd'), 4)
        self.assertEqual([(fields(p)[0][2], fields(p)[1]) for p in sock.sent],
                         [(2001, b'abcd'), (2003, b'cd')])

    def test_zero_window_probe_reopens_without_consuming_sequence(self):
        self.state.peer_rwnd = 0

        def respond(packet):
            header, payload = fields(packet)
            if header[2] == 2000:
                self.deliver(sock)
            else:
                self.deliver(sock, ack=header[2] + len(payload))

        sock = FakeSocket(respond)
        packets = ScriptedQueue([])
        with scripted_transport(raw, self.key, packets):
            self.assertEqual(raw.run_conn_send(sock, self.connection, b'abc'), 3)
        self.assertEqual([(fields(p)[0][2], fields(p)[1]) for p in sock.sent],
                         [(2000, b'\x00'), (2001, b'abc')])
        self.assertEqual(raw.connections[self.key].server_seq, 2004)
        self.assertEqual(self.state.cc.cwnd, 2 * MSS)
        self.assertEqual(packets.timeouts, [1.0])

    def test_zero_window_stops_after_five_probes_without_congestion_loss(self):
        self.state.peer_rwnd = 0
        self.state.cc.cwnd = 10.0 * MSS
        sock = FakeSocket()
        packets = ScriptedQueue([])
        with scripted_transport(raw, self.key, packets):
            self.assertEqual(raw.run_conn_send(sock, self.connection, b'abc'), -1)
        self.assertEqual(len(sock.sent), 5)
        self.assertTrue(all(fields(p)[0][2] == 2000 for p in sock.sent))
        self.assertEqual(self.state.cc.cwnd, 10 * MSS)
        self.assertEqual(raw.connections[self.key].server_seq, 2001)
        self.assertEqual(raw.run_conn_send(sock, self.connection, b'next'), -1)

    def test_three_duplicate_acks_retransmit_before_timeout(self):
        self.state.cc.cwnd = 10.0 * MSS

        def respond(packet):
            if len(sock.sent) == 1:
                for _ in range(3):
                    self.deliver(sock)
            else:
                self.deliver(sock, ack=2004)

        sock = FakeSocket(respond)
        packets = ScriptedQueue([])
        with scripted_transport(raw, self.key, packets):
            self.assertEqual(raw.run_conn_send(sock, self.connection, b'abc'), 3)
        self.assertEqual(len(sock.sent), 2)
        self.assertEqual([fields(p)[0][2] for p in sock.sent], [2001, 2001])
        self.assertEqual(packets.timeouts, [])
        self.assertAlmostEqual(self.state.cc.ssthresh, 7 * MSS)

    def test_window_updates_do_not_trigger_fast_retransmit(self):
        def respond(packet):
            for window in (4000, 3000, 2000):
                self.deliver(sock, window=window)
            self.deliver(sock, ack=2004)

        sock = FakeSocket(respond)
        self.assertEqual(raw.run_conn_send(sock, self.connection, b'abc'), 3)
        self.assertEqual(len(sock.sent), 1)
        self.assertEqual(self.state.cc.ssthresh, 65536)

    def test_ack_beyond_sent_data_cannot_close_window(self):
        def respond(packet):
            self.deliver(sock, ack=3000, window=0)
            self.assertEqual(self.state.peer_rwnd, MAX_WINDOW)
            self.deliver(sock, ack=2004)

        sock = FakeSocket(respond)
        self.assertEqual(raw.run_conn_send(sock, self.connection, b'abc'), 3)

    def test_receive_clips_to_buffer_and_announces_reopening(self):
        self.state.rcv = ReceiveBuffer(3)
        sock = FakeSocket()
        self.deliver(sock, payload=b'hello')
        self.assertEqual(fields(sock.sent[0])[0][3], 104)
        self.assertEqual(fields(sock.sent[0])[0][6], 0)
        self.assertEqual(raw.run_conn_recv(sock, self.connection), b'hel')
        self.assertEqual(fields(sock.sent[1])[0][3], 104)
        self.assertEqual(fields(sock.sent[1])[0][6], 3)
        self.deliver(sock, seq=104, payload=b'lo')
        self.assertEqual(raw.run_conn_recv(sock, self.connection), b'lo')
        self.assertEqual(raw.connections[self.key].client_seq, 106)

    def test_fin_preserves_buffered_data_before_eof(self):
        sock = FakeSocket()
        self.deliver(sock, payload=b'ok', flags=17)
        self.assertEqual(fields(sock.sent[0])[0][3], 104)
        self.assertEqual(raw.run_conn_recv(sock, self.connection), b'ok')
        self.assertEqual(raw.run_conn_recv(sock, self.connection), b'')
        self.assertEqual(raw.run_conn_send(sock, self.connection, b'next'), -1)

    def test_reset_wakes_blocked_receiver(self):
        sock = FakeSocket()
        ready = threading.Event()
        result = []

        def receive():
            ready.set()
            result.append(raw.run_conn_recv(sock, self.connection))

        thread = threading.Thread(target=receive, daemon=True)
        thread.start()
        self.assertTrue(ready.wait(1))
        self.deliver(sock, flags=4)
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result, [b''])

    def test_full_duplex_and_concurrent_send_guard(self):
        sent = threading.Event()
        sock = FakeSocket(lambda packet: sent.set())
        result = []
        received = []
        sender = threading.Thread(
            target=lambda: result.append(raw.run_conn_send(sock, self.connection, b'abc')),
            daemon=True)
        receiver = threading.Thread(
            target=lambda: received.append(raw.run_conn_recv(sock, self.connection)), daemon=True)
        sender.start()
        self.assertTrue(sent.wait(1))
        receiver.start()
        self.assertEqual(raw.run_conn_send(sock, self.connection, b'other'), -1)
        self.deliver(sock, ack=2004, payload=b'reply')
        sender.join(1)
        receiver.join(1)
        self.assertFalse(sender.is_alive())
        self.assertFalse(receiver.is_alive())
        self.assertEqual(result, [3])
        self.assertEqual(received, [b'reply'])

    def test_close_wakes_sender_and_receiver(self):
        sent = threading.Event()
        sock = FakeSocket(lambda packet: sent.set())
        result = []
        sender = threading.Thread(
            target=lambda: result.append(raw.run_conn_send(sock, self.connection, b'abc')),
            daemon=True)
        sender.start()
        self.assertTrue(sent.wait(1))
        raw.close_connection(sock, self.connection)
        sender.join(1)
        self.assertFalse(sender.is_alive())
        self.assertEqual(result, [-1])
        self.assertEqual(raw.run_conn_recv(sock, self.connection), b'')

    def test_segment_checksum_is_valid(self):
        def respond(packet):
            header, payload = fields(packet)
            pseudo = struct.pack('!4s4sBBH', packet[12:16], packet[16:20],
                                 0, 6, len(packet) - 20)
            self.assertEqual(raw.checksum(pseudo + packet[20:]), 0)
            self.deliver(sock, ack=header[2] + len(payload))

        sock = FakeSocket(respond)
        self.assertEqual(raw.run_conn_send(sock, self.connection, b'x' * (MSS + 1)), MSS + 1)


class LinkSocket:
    def __init__(self, network):
        self.network = network
        self.incoming = queue.Queue()
        self.timeout = 0.1
        self.sent = []
        self.drop = None

    def settimeout(self, timeout):
        self.timeout = timeout

    def recvfrom(self, size):
        try:
            return self.incoming.get(timeout=self.timeout)
        except queue.Empty:
            raise socket.timeout()

    def sendto(self, packet, address):
        self.sent.append(packet)
        header, payload = fields(packet)
        if self.drop is None or not self.drop(header, payload):
            self.network[header[1]].incoming.put((packet, (address[0], 0)))
        return len(packet)


class DispatcherTransferTests(unittest.TestCase):
    def test_large_transfer_with_slow_reader_lost_segment_and_lost_ack(self):
        raw.close_all_connections()
        network = {}
        left = network[9000] = LinkSocket(network)
        right = network[40000] = LinkSocket(network)
        left_key = ('127.0.0.1', 40000)
        right_key = ('127.0.0.2', 9000)
        left_conn = raw.conn_struct('127.0.0.1', 40000, '127.0.0.2', 9000,
                                   'ESTABLISHED', 0xFFFFFFFE, 101, 0, time.time())
        right_conn = raw.conn_struct('127.0.0.2', 9000, '127.0.0.1', 40000,
                                    'ESTABLISHED', 101, 0xFFFFFFFE, 0, time.time())
        raw.connections[left_key] = left_conn
        raw.connections[right_key] = right_conn
        left_state = raw.create_tcp_state(left_key, peer_rwnd=2 * MSS)
        right_state = raw.create_tcp_state(right_key)
        left_state.cc.cwnd = 4.0 * MSS
        right_state.rcv = ReceiveBuffer(2 * MSS)
        raw.activate_connection(left, left_key)
        raw.activate_connection(right, right_key)
        losses = [False, False]

        def lose_segment(header, payload):
            if payload and header[2] == (0xFFFFFFFE + MSS) % (2 ** 32) and not losses[0]:
                losses[0] = True
                return True
            return False

        def lose_ack(header, payload):
            if not payload and header[3] != 0xFFFFFFFE and not losses[1]:
                losses[1] = True
                return True
            return False

        left.drop = lose_segment
        right.drop = lose_ack
        payload = bytes(range(256)) * 300
        sent = []
        received = bytearray()
        dispatcher._packet_dispatcher_running = True
        threads = [threading.Thread(target=dispatcher._packet_dispatcher,
                                    args=(sock, port), daemon=True)
                   for port, sock in network.items()]
        for thread in threads:
            thread.start()
        sender = threading.Thread(
            target=lambda: sent.append(raw.run_conn_send(left, left_conn, payload)), daemon=True)
        try:
            sender.start()
            time.sleep(0.05)
            deadline = time.monotonic() + 15
            while len(received) < len(payload) and time.monotonic() < deadline:
                received.extend(raw.run_conn_recv(right, right_conn, timeout=0.5))
                time.sleep(0.001)
            sender.join(1)
            self.assertFalse(sender.is_alive())
            self.assertEqual(sent, [len(payload)])
            self.assertEqual(bytes(received), payload)
            self.assertEqual(losses, [True, True])
            self.assertTrue(any(fields(p)[0][6] == 0 for p in right.sent))
            self.assertTrue(all(len(fields(p)[1]) <= MSS for p in left.sent))
        finally:
            dispatcher._packet_dispatcher_running = False
            raw.close_all_connections()
            sender.join(1)
            for thread in threads:
                thread.join(1)


if __name__ == '__main__':
    unittest.main()
