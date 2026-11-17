import contextlib
import io
import os
import socket
import struct
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from streaprous_socket import streaprous_raw_connection as rc
from streaprous_socket import rtt_estimator as rtt_mod
from streaprous_socket import sender as sender_mod
from streaprous_socket.streaprous_packet_dispatcher import get_packet_queue, remove_packet_queue

SERVER_IP = "10.0.0.1"
SERVER_PORT = 9000
CLIENT_IP = "10.0.0.2"
CLIENT_ISN = 5000
MSS = 1460


def client_packet(client_port, seq, payload=b"", ack=0, window=65535, flags=0x18, options=b""):
    offset = (20 + len(options)) // 4
    tcp = struct.pack('!HHLLBBHHH', client_port, SERVER_PORT, seq % rc.SEQ_MOD, ack % rc.SEQ_MOD,
                      offset << 4, flags, window, 0, 0) + options
    ip = rc.build_ip_header(CLIENT_IP, SERVER_IP, socket.IPPROTO_TCP, len(tcp) + len(payload))
    return ip + tcp + payload


def run_with_timeout(testcase, fn, timeout=10.0):
    result = {}

    def target():
        try:
            result["value"] = fn()
        except BaseException as e:
            result["error"] = e

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        testcase.fail("call did not finish within {}s".format(timeout))
    if "error" in result:
        raise result["error"]
    return result.get("value")


class FakePeer:

    def __init__(self, client_port, server_isn, window=65535, drop_offsets=(),
                 drop_all=False, zero_window_for=0.0):
        self.client_port = client_port
        self.base = server_isn
        self.window = window
        self.drop_offsets = set(drop_offsets)
        self.drop_all = drop_all
        self.zero_until = time.time() + zero_window_for
        self.data = bytearray()
        self.ooo = {}
        self.segments = []
        self.acks = []
        self.max_outstanding = 0

    def current_window(self):
        return 0 if time.time() < self.zero_until else self.window

    def sendto(self, packet, addr):
        seg = rc.parse_tcp_segment(packet)
        if not seg.payload:
            return len(packet)
        off = rc.seq_diff(seg.seq, self.base)
        self.segments.append((off, len(seg.payload)))
        last_ack = len(self.data)
        self.max_outstanding = max(self.max_outstanding, off + len(seg.payload) - last_ack)

        if self.drop_all:
            return len(packet)
        if off in self.drop_offsets:
            self.drop_offsets.discard(off)
            return len(packet)

        window = self.current_window()
        if window > 0:
            if off == len(self.data):
                self.data += seg.payload
                while len(self.data) in self.ooo:
                    self.data += self.ooo.pop(len(self.data))
            elif off > len(self.data):
                self.ooo[off] = seg.payload
            elif off + len(seg.payload) > len(self.data):
                self.data += seg.payload[len(self.data) - off:]
        self._send_ack(window)
        return len(packet)

    def _send_ack(self, window):
        ack_off = len(self.data)
        self.acks.append(ack_off)
        pkt = client_packet(self.client_port, CLIENT_ISN, ack=rc.seq_add(self.base, ack_off),
                            window=window, flags=0x10)
        get_packet_queue((CLIENT_IP, self.client_port)).put((pkt, (CLIENT_IP, 0)))


class RecordingSocket:

    def __init__(self):
        self.sent = []

    def sendto(self, packet, addr):
        self.sent.append(rc.parse_tcp_segment(packet))
        return len(packet)


class ConnectionTestCase(unittest.TestCase):

    _next_port = 40000

    def setUp(self):
        self._saved = (rtt_mod.TCP_INITIAL_RTO, rtt_mod.TCP_MIN_RTO, sender_mod.TCP_MAX_RETRIES)
        rtt_mod.TCP_INITIAL_RTO = 0.3
        rtt_mod.TCP_MIN_RTO = 0.2
        self._keys = []
        out = contextlib.redirect_stdout(io.StringIO())
        out.__enter__()
        self.addCleanup(out.__exit__, None, None, None)

    def tearDown(self):
        rtt_mod.TCP_INITIAL_RTO, rtt_mod.TCP_MIN_RTO, sender_mod.TCP_MAX_RETRIES = self._saved
        for key in self._keys:
            with rc.connections_lock:
                rc.connections.pop(key, None)
            rc.drop_tcp_state(key)
            remove_packet_queue(key)

    def make_connection(self, server_isn=2001, peer_rwnd=65535, peer_mss=MSS):
        ConnectionTestCase._next_port += 1
        port = ConnectionTestCase._next_port
        key = (CLIENT_IP, port)
        conn = rc.conn_struct(
            client_ip=CLIENT_IP, client_port=port, server_ip=SERVER_IP, server_port=SERVER_PORT,
            state="ESTABLISHED", server_seq=server_isn, client_seq=CLIENT_ISN,
            expected_final_ack=0, last_activity=time.time())
        with rc.connections_lock:
            rc.connections[key] = conn
        state = rc.create_tcp_state(key, rcv_nxt=CLIENT_ISN, peer_rwnd=peer_rwnd, peer_mss=peer_mss)
        self._keys.append(key)
        return conn, state


class TestReceiveBuffer(unittest.TestCase):

    def test_in_order_data_reduces_rwnd_until_read(self):
        buf = rc.ReceiveBuffer(capacity=1000, rcv_nxt=100)
        self.assertEqual(buf.receive(100, b"a" * 300), 300)
        self.assertEqual(buf.rcv_nxt, 400)
        self.assertEqual(buf.rwnd, 700)
        self.assertEqual(buf.read(), b"a" * 300)
        self.assertEqual(buf.rwnd, 1000)

    def test_out_of_order_segment_is_buffered_until_gap_filled(self):
        buf = rc.ReceiveBuffer(capacity=1000, rcv_nxt=0)
        self.assertEqual(buf.receive(5, b"world"), 0)
        self.assertEqual(buf.rcv_nxt, 0)
        self.assertTrue(buf.has_gap)
        self.assertEqual(buf.read(), b"")
        self.assertEqual(buf.receive(0, b"hello"), 10)
        self.assertEqual(buf.rcv_nxt, 10)
        self.assertFalse(buf.has_gap)
        self.assertEqual(buf.read(), b"helloworld")

    def test_multiple_out_of_order_segments_merge(self):
        buf = rc.ReceiveBuffer(capacity=1000, rcv_nxt=0)
        buf.receive(20, b"C" * 10)
        buf.receive(10, b"B" * 10)
        self.assertEqual(buf.receive(0, b"A" * 10), 30)
        self.assertEqual(buf.read(), b"A" * 10 + b"B" * 10 + b"C" * 10)

    def test_duplicate_and_overlapping_data_is_trimmed(self):
        buf = rc.ReceiveBuffer(capacity=1000, rcv_nxt=0)
        buf.receive(0, b"hello")
        self.assertEqual(buf.receive(0, b"hello"), 0)
        self.assertEqual(buf.receive(3, b"loworld"), 5)
        self.assertEqual(buf.read(), b"helloworld")

    def test_data_beyond_window_is_cut(self):
        buf = rc.ReceiveBuffer(capacity=10, rcv_nxt=0)
        self.assertEqual(buf.receive(0, b"x" * 15), 10)
        self.assertEqual(buf.rwnd, 0)
        self.assertEqual(buf.receive(10, b"y"), 0)
        self.assertEqual(buf.read(), b"x" * 10)

    def test_window_right_edge_never_shrinks(self):
        buf = rc.ReceiveBuffer(capacity=1000, rcv_nxt=0)
        edge = buf.rcv_nxt + buf.advertise()
        buf.receive(0, b"a" * 400)
        self.assertEqual(buf.rcv_nxt + buf.advertise(), edge)
        buf.receive(600, b"b" * 100)
        self.assertEqual(buf.rcv_nxt + buf.advertise(), edge)

    def test_sequence_number_wraparound(self):
        start = rc.SEQ_MOD - 50
        buf = rc.ReceiveBuffer(capacity=1000, rcv_nxt=start)
        buf.receive(rc.seq_add(start, 50), b"B" * 50)
        self.assertEqual(buf.receive(start, b"A" * 50), 100)
        self.assertEqual(buf.rcv_nxt, 50)
        self.assertEqual(buf.read(), b"A" * 50 + b"B" * 50)

    def test_window_update_only_when_window_opens_enough(self):
        buf = rc.ReceiveBuffer(capacity=10000, rcv_nxt=0, mss=1000)
        buf.receive(0, b"a" * 5000)
        buf.advertise()
        buf.read(500)
        self.assertFalse(buf.window_update_needed())
        buf.read(600)
        self.assertTrue(buf.window_update_needed())
        buf.advertise()
        self.assertFalse(buf.window_update_needed())

    def test_capacity_limited_to_16bit_window(self):
        buf = rc.ReceiveBuffer(capacity=10 ** 6)
        self.assertEqual(buf.capacity, rc.MAX_WINDOW)


class TestPacketParsing(unittest.TestCase):

    def test_parse_mss_option(self):
        options = b"\x02\x04\x05\xb4" + b"\x01\x01\x04\x02"
        pkt = client_packet(1234, 1, flags=0x02, options=options)
        seg = rc.parse_tcp_segment(pkt)
        self.assertEqual(rc.parse_mss_option(seg.header), 1460)
        self.assertEqual(seg.payload, b"")

    def test_no_mss_option(self):
        seg = rc.parse_tcp_segment(client_packet(1234, 1, flags=0x02))
        self.assertIsNone(rc.parse_mss_option(seg.header))

    def test_window_field_written_into_header(self):
        hdr = rc.build_tcp_packet(SERVER_IP, CLIENT_IP, 1, 2, seq=1, ack_seq=2, ack=1, window=1234)
        self.assertEqual(struct.unpack('!H', hdr[14:16])[0], 1234)

    def test_state_uses_smaller_of_our_and_peer_mss(self):
        self.assertEqual(rc.TcpControlState(peer_mss=536).mss, 536)
        self.assertEqual(rc.TcpControlState(peer_mss=9000).mss, rc.DEFAULT_MSS)


def run_rtts(cc, rtt, n, start_time):
    now = start_time
    for _ in range(n):
        now += rtt
        cc.on_ack(int(cc.cwnd * cc.mss), now, rtt)
    return now


class TestCubicSlowStart(unittest.TestCase):

    def test_initial_window(self):
        cc = rc.CubicCC(mss=MSS)
        self.assertEqual(cc.cwnd, rc.INITIAL_CWND_SEGMENTS)
        self.assertEqual(cc.cwnd_bytes, rc.INITIAL_CWND_SEGMENTS * MSS)
        self.assertTrue(cc.in_slow_start)

    def test_slow_start_doubles_every_rtt(self):
        cc = rc.CubicCC(mss=MSS, initial_cwnd=10)
        run_rtts(cc, 0.1, 1, 0.0)
        self.assertAlmostEqual(cc.cwnd, 20)
        run_rtts(cc, 0.1, 1, 0.1)
        self.assertAlmostEqual(cc.cwnd, 40)

    def test_one_segment_per_acked_segment(self):
        cc = rc.CubicCC(mss=MSS, initial_cwnd=10)
        cc.on_ack(MSS, 0.0)
        self.assertAlmostEqual(cc.cwnd, 11)

    def test_slow_start_stops_at_ssthresh(self):
        cc = rc.CubicCC(mss=MSS, initial_cwnd=10)
        cc.ssthresh = 15
        cc.on_ack(10 * MSS, 0.0, 0.1)
        self.assertGreaterEqual(cc.cwnd, 15)
        self.assertLess(cc.cwnd, 16)
        self.assertFalse(cc.in_slow_start)


class TestCubicCongestionAvoidance(unittest.TestCase):

    def make_after_loss(self, cwnd=100):
        cc = rc.CubicCC(mss=MSS, initial_cwnd=cwnd)
        cc.ssthresh = cwnd
        cc.on_loss(0.0)
        return cc

    def test_k_is_time_to_return_to_w_max(self):
        cc = self.make_after_loss()
        cc.on_ack(MSS, 0.0, 0.1)
        expected_k = ((100 - 70) / rc.CUBIC_C) ** (1 / 3)
        self.assertAlmostEqual(cc.k, expected_k, places=3)
        self.assertAlmostEqual(cc.w_cubic(cc.k), cc.w_max)

    def test_concave_then_plateau_then_convex(self):
        cc = self.make_after_loss()
        rtt = 0.1
        now = run_rtts(cc, rtt, 10, 0.0)
        early = cc.cwnd
        self.assertGreater(early, 70)
        self.assertLess(early, 100)

        now = run_rtts(cc, rtt, int(cc.k / rtt) - 10, now)
        self.assertAlmostEqual(cc.cwnd, 100, delta=3)

        before = cc.cwnd
        run_rtts(cc, rtt, 30, now)
        self.assertGreater(cc.cwnd, before + 5)

    def test_growth_slows_down_near_w_max(self):
        cc = self.make_after_loss()
        rtt = 0.1
        start = cc.cwnd
        now = run_rtts(cc, rtt, 10, 0.0)
        first_second = cc.cwnd - start
        mid = cc.cwnd
        run_rtts(cc, rtt, 10, now + 2.0)
        near_k = cc.cwnd - mid
        self.assertGreater(first_second, 0)
        later_rate = near_k / 3.0
        self.assertLess(later_rate, first_second)

    def test_reno_friendly_region_after_loss(self):
        cc = rc.CubicCC(mss=MSS, initial_cwnd=100, c=1e-6)
        cc.ssthresh = 100
        cc.on_loss(0.0)
        run_rtts(cc, 0.1, 10, 0.0)
        alpha = 3 * (1 - rc.CUBIC_BETA) / (1 + rc.CUBIC_BETA)
        self.assertAlmostEqual(cc.cwnd, 70 + 10 * alpha, delta=0.5)

    def test_reno_friendly_alpha_is_one_above_w_max(self):
        cc = rc.CubicCC(mss=MSS, initial_cwnd=20, c=1e-6)
        cc.ssthresh = 20
        run_rtts(cc, 0.1, 10, 0.0)
        self.assertAlmostEqual(cc.cwnd, 30, delta=0.5)

    def test_cwnd_never_decreases_on_ack(self):
        cc = self.make_after_loss()
        now = 0.0
        prev = cc.cwnd
        for _ in range(200):
            now += 0.05
            cc.on_ack(MSS, now, 0.1)
            self.assertGreaterEqual(cc.cwnd, prev)
            prev = cc.cwnd


class TestLossReaction(unittest.TestCase):

    def test_triple_dupack_multiplicative_decrease(self):
        cc = rc.CubicCC(mss=MSS, initial_cwnd=100)
        cc.on_loss(1.0)
        self.assertAlmostEqual(cc.w_max, 100)
        self.assertAlmostEqual(cc.cwnd, 70)
        self.assertAlmostEqual(cc.ssthresh, 70)
        self.assertFalse(cc.in_slow_start)
        self.assertEqual(cc.loss_events, 1)

    def test_fast_convergence(self):
        cc = rc.CubicCC(mss=MSS, initial_cwnd=100)
        cc.on_loss(0.0)
        cc.cwnd = 80
        cc.on_loss(1.0)
        self.assertAlmostEqual(cc.w_max, 80 * (1 + rc.CUBIC_BETA) / 2)
        self.assertAlmostEqual(cc.cwnd, 56)

    def test_ssthresh_at_least_two_segments(self):
        cc = rc.CubicCC(mss=MSS, initial_cwnd=2)
        cc.on_loss(0.0)
        self.assertEqual(cc.ssthresh, 2.0)

    def test_rto_resets_cwnd_to_one_segment(self):
        cc = rc.CubicCC(mss=MSS, initial_cwnd=40)
        cc.on_timeout(first=True)
        self.assertEqual(cc.cwnd, 1.0)
        self.assertAlmostEqual(cc.ssthresh, 28)
        self.assertTrue(cc.in_slow_start)
        self.assertEqual(cc.timeouts, 1)

    def test_repeated_rto_keeps_ssthresh(self):
        cc = rc.CubicCC(mss=MSS, initial_cwnd=40)
        cc.on_timeout(first=True)
        cc.on_timeout(first=False)
        self.assertAlmostEqual(cc.ssthresh, 28)
        self.assertEqual(cc.cwnd, 1.0)

    def test_slow_start_after_rto_then_cubic(self):
        cc = rc.CubicCC(mss=MSS, initial_cwnd=40)
        cc.on_timeout()
        run_rtts(cc, 0.1, 5, 0.0)
        self.assertFalse(cc.in_slow_start)
        self.assertLess(cc.cwnd, 40)


class TestRttEstimator(unittest.TestCase):

    def test_first_sample(self):
        r = rc.RttEstimator(initial_rto=1.0, min_rto=0.0)
        self.assertEqual(r.rto, 1.0)
        r.on_sample(0.1)
        self.assertAlmostEqual(r.srtt, 0.1)
        self.assertAlmostEqual(r.rttvar, 0.05)
        self.assertAlmostEqual(r.rto, 0.1 + 4 * 0.05)

    def test_subsequent_samples(self):
        r = rc.RttEstimator(min_rto=0.0)
        r.on_sample(0.1)
        r.on_sample(0.3)
        self.assertAlmostEqual(r.rttvar, 0.75 * 0.05 + 0.25 * 0.2)
        self.assertAlmostEqual(r.srtt, 0.875 * 0.1 + 0.125 * 0.3)

    def test_min_rto(self):
        r = rc.RttEstimator(min_rto=0.2)
        r.on_sample(0.001)
        self.assertEqual(r.rto, 0.2)

    def test_backoff_doubles_and_is_capped(self):
        r = rc.RttEstimator(initial_rto=1.0, max_rto=5.0)
        r.backoff()
        self.assertEqual(r.rto, 2.0)
        for _ in range(10):
            r.backoff()
        self.assertEqual(r.rto, 5.0)


class TestRunConnSend(ConnectionTestCase):

    def send(self, conn, peer, payload, timeout=10.0):
        return run_with_timeout(self, lambda: rc.run_conn_send(peer, conn, payload), timeout)

    def test_no_loss_delivers_data_and_slow_start_grows_cwnd(self):
        conn, state = self.make_connection()
        peer = FakePeer(conn.client_port, conn.server_seq)
        payload = os.urandom(100 * 1024)
        self.assertEqual(self.send(conn, peer, payload), len(payload))
        self.assertEqual(bytes(peer.data), payload)
        self.assertGreater(state.cc.cwnd, rc.INITIAL_CWND_SEGMENTS)
        self.assertEqual(state.cc.loss_events, 0)
        self.assertEqual(state.cc.timeouts, 0)
        self.assertEqual(rc.connections[(CLIENT_IP, conn.client_port)].server_seq,
                         rc.seq_add(conn.server_seq, len(payload)))

    def test_segments_respect_mss(self):
        conn, state = self.make_connection(peer_mss=536)
        peer = FakePeer(conn.client_port, conn.server_seq)
        self.send(conn, peer, b"x" * 5000)
        self.assertTrue(all(length <= 536 for _, length in peer.segments))

    def test_initial_burst_limited_by_cwnd(self):
        conn, state = self.make_connection()
        peer = FakePeer(conn.client_port, conn.server_seq, drop_all=True)
        sender_mod.TCP_MAX_RETRIES = 0
        self.send(conn, peer, b"x" * 50 * MSS)
        first_burst = [s for s in peer.segments if s[0] < rc.INITIAL_CWND_SEGMENTS * MSS]
        self.assertEqual(peer.segments[:len(first_burst)], first_burst)
        self.assertEqual(len(first_burst), rc.INITIAL_CWND_SEGMENTS)

    def test_flow_control_inflight_never_exceeds_rwnd(self):
        conn, state = self.make_connection(peer_rwnd=3000)
        peer = FakePeer(conn.client_port, conn.server_seq, window=3000)
        payload = os.urandom(30000)
        self.assertEqual(self.send(conn, peer, payload), len(payload))
        self.assertEqual(bytes(peer.data), payload)
        self.assertLessEqual(peer.max_outstanding, 3000)

    def test_fast_retransmit_on_three_duplicate_acks(self):
        conn, state = self.make_connection()
        lost = 2 * MSS
        peer = FakePeer(conn.client_port, conn.server_seq, drop_offsets=[lost])
        payload = os.urandom(20 * MSS)
        self.assertEqual(self.send(conn, peer, payload), len(payload))
        self.assertEqual(bytes(peer.data), payload)
        self.assertEqual(state.cc.loss_events, 1)
        self.assertEqual(state.cc.timeouts, 0)
        self.assertLess(state.cc.ssthresh, float("inf"))
        self.assertEqual(sum(1 for off, _ in peer.segments if off == lost), 2)

    def test_rto_when_last_segment_is_lost(self):
        conn, state = self.make_connection()
        peer = FakePeer(conn.client_port, conn.server_seq, drop_offsets=[2 * MSS])
        payload = os.urandom(3 * MSS)
        self.assertEqual(self.send(conn, peer, payload), len(payload))
        self.assertEqual(bytes(peer.data), payload)
        self.assertEqual(state.cc.timeouts, 1)
        self.assertEqual(state.cc.loss_events, 0)
        self.assertAlmostEqual(state.cc.ssthresh, 12 * rc.CUBIC_BETA)
        self.assertAlmostEqual(state.cc.cwnd, 2.0)
        self.assertGreaterEqual(state.rtt.rto, 2 * rtt_mod.TCP_MIN_RTO)

    def test_sequence_numbers_wrap_around(self):
        conn, state = self.make_connection(server_isn=rc.SEQ_MOD - 3000)
        peer = FakePeer(conn.client_port, conn.server_seq, drop_offsets=[2 * MSS])
        payload = os.urandom(10 * MSS)
        self.assertEqual(self.send(conn, peer, payload), len(payload))
        self.assertEqual(bytes(peer.data), payload)

    def test_zero_window_uses_persist_probes_without_congestion_reaction(self):
        conn, state = self.make_connection(peer_rwnd=0)
        peer = FakePeer(conn.client_port, conn.server_seq, zero_window_for=0.4)
        payload = os.urandom(5 * MSS)
        self.assertEqual(self.send(conn, peer, payload), len(payload))
        self.assertEqual(bytes(peer.data), payload)
        self.assertEqual(peer.segments[0], (0, 1))
        self.assertEqual(state.cc.timeouts, 0)
        self.assertEqual(state.cc.loss_events, 0)

    def test_abort_after_max_retries(self):
        rtt_mod.TCP_INITIAL_RTO = 0.02
        rtt_mod.TCP_MIN_RTO = 0.02
        sender_mod.TCP_MAX_RETRIES = 2
        conn, state = self.make_connection()
        peer = FakePeer(conn.client_port, conn.server_seq, drop_all=True)
        self.assertEqual(self.send(conn, peer, b"x" * 1000), -1)
        self.assertEqual(state.cc.timeouts, 2)

    def test_unknown_connection(self):
        conn, _ = self.make_connection()
        other = conn._replace(client_port=1)
        self.assertEqual(rc.run_conn_send(RecordingSocket(), other, b"data"), -1)


class TestRunConnRecv(ConnectionTestCase):

    def recv(self, conn, sock):
        return run_with_timeout(self, lambda: rc.run_conn_recv(sock, conn), 5.0)

    def push(self, conn, seq, payload=b"", window=65535, flags=0x18):
        pkt = client_packet(conn.client_port, seq, payload, ack=conn.server_seq, window=window, flags=flags)
        get_packet_queue((CLIENT_IP, conn.client_port)).put((pkt, (CLIENT_IP, 0)))

    def test_in_order_data_is_acked_with_rwnd(self):
        conn, state = self.make_connection()
        sock = RecordingSocket()
        self.push(conn, CLIENT_ISN, b"GET / HTTP/1.1\r\n\r\n")
        self.assertEqual(self.recv(conn, sock), b"GET / HTTP/1.1\r\n\r\n")
        ack = sock.sent[0]
        self.assertEqual(ack.ack, CLIENT_ISN + 18)
        self.assertEqual(ack.window, rc.RECV_BUFFER_SIZE - 18)

    def test_out_of_order_produces_duplicate_ack_then_full_data(self):
        conn, state = self.make_connection()
        sock = RecordingSocket()
        self.push(conn, CLIENT_ISN + 5, b"world")
        self.push(conn, CLIENT_ISN, b"hello")
        self.assertEqual(self.recv(conn, sock), b"helloworld")
        acks = [seg.ack for seg in sock.sent]
        self.assertEqual(acks[0], CLIENT_ISN)
        self.assertEqual(acks[1], CLIENT_ISN + 10)

    def test_duplicate_segment_is_not_delivered_twice(self):
        conn, state = self.make_connection()
        sock = RecordingSocket()
        self.push(conn, CLIENT_ISN, b"hello")
        self.assertEqual(self.recv(conn, sock), b"hello")
        self.push(conn, CLIENT_ISN, b"hello")
        self.push(conn, CLIENT_ISN + 5, b"world")
        self.assertEqual(self.recv(conn, sock), b"world")
        self.assertEqual(sock.sent[1].ack, CLIENT_ISN + 5)

    def test_records_client_window(self):
        conn, state = self.make_connection()
        self.push(conn, CLIENT_ISN, b"data", window=1234)
        self.recv(conn, RecordingSocket())
        self.assertEqual(state.peer_rwnd, 1234)

    def test_window_update_after_large_read(self):
        conn, state = self.make_connection(peer_mss=1000)
        sock = RecordingSocket()
        self.push(conn, CLIENT_ISN, b"a" * 1000)
        self.recv(conn, sock)
        self.assertEqual(len(sock.sent), 2)
        self.assertEqual(sock.sent[1].window, rc.RECV_BUFFER_SIZE)

    def test_fin_is_acked_and_connection_removed(self):
        conn, state = self.make_connection()
        sock = RecordingSocket()
        self.push(conn, CLIENT_ISN, b"", flags=0x11)
        self.assertEqual(self.recv(conn, sock), b"")
        self.assertEqual(sock.sent[0].ack, CLIENT_ISN + 1)
        self.assertNotIn((CLIENT_IP, conn.client_port), rc.connections)
        self.assertIsNone(rc.get_tcp_state((CLIENT_IP, conn.client_port)))


if __name__ == "__main__":
    unittest.main()
