import os
import struct
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from streaprous_socket import streaprous_raw_connection as rc
from streaprous_socket.flow_control import ReceiveBuffer, usable_window, RCV_BUFFER, MAX_WINDOW
from streaprous_socket.congestion_control import CubicCC, MSS, INITIAL_SSTHRESH, CUBIC_C, CUBIC_BETA
from streaprous_socket.tcp_state import (
    TcpControlState, create_tcp_state, get_tcp_state, update_peer_rwnd, drop_tcp_state,
)


class TestReceiveBuffer(unittest.TestCase):

    def test_initial_rwnd_is_rcv_buffer(self):
        buf = ReceiveBuffer(4096)
        self.assertEqual(buf.rwnd, 4096)

    def test_rwnd_formula(self):
        buf = ReceiveBuffer(4096)
        buf.on_data(b"a" * 1000)
        buf.read(300)
        self.assertEqual(buf.last_byte_rcvd, 1000)
        self.assertEqual(buf.last_byte_read, 300)
        self.assertEqual(buf.rwnd, 4096 - (1000 - 300))

    def test_read_frees_buffer(self):
        buf = ReceiveBuffer(4096)
        buf.on_data(b"hello")
        self.assertEqual(buf.rwnd, 4091)
        self.assertEqual(buf.read(), b"hello")
        self.assertEqual(buf.rwnd, 4096)

    def test_data_beyond_rwnd_is_dropped(self):
        buf = ReceiveBuffer(10)
        self.assertEqual(buf.on_data(b"x" * 15), 10)
        self.assertEqual(buf.rwnd, 0)
        self.assertEqual(buf.on_data(b"y"), 0)
        self.assertEqual(buf.read(), b"x" * 10)

    def test_rcv_buffer_limited_to_16_bit_window(self):
        self.assertEqual(ReceiveBuffer(10 ** 6).rcv_buffer, MAX_WINDOW)
        self.assertEqual(ReceiveBuffer().rcv_buffer, RCV_BUFFER)

    def test_advertise_returns_rwnd(self):
        buf = ReceiveBuffer(100)
        buf.on_data(b"a" * 40)
        self.assertEqual(buf.advertise(), 60)

    def test_window_update_after_zero_window(self):
        buf = ReceiveBuffer(10)
        buf.on_data(b"x" * 10)
        self.assertEqual(buf.advertise(), 0)
        self.assertFalse(buf.window_update_needed())
        buf.read(4)
        self.assertTrue(buf.window_update_needed())
        buf.advertise()
        self.assertFalse(buf.window_update_needed())

    def test_no_window_update_when_window_was_open(self):
        buf = ReceiveBuffer(100)
        buf.on_data(b"a" * 50)
        buf.advertise()
        buf.read()
        self.assertFalse(buf.window_update_needed())


class TestUsableWindow(unittest.TestCase):

    def test_limited_by_cwnd(self):
        self.assertEqual(usable_window(1000, 1000, cwnd=2000, rwnd=5000), 2000)

    def test_limited_by_rwnd(self):
        self.assertEqual(usable_window(1000, 1000, cwnd=5000, rwnd=3000), 3000)

    def test_subtracts_inflight(self):
        self.assertEqual(usable_window(1500, 1000, cwnd=2000, rwnd=5000), 1500)

    def test_full_window(self):
        self.assertEqual(usable_window(3000, 1000, cwnd=2000, rwnd=5000), 0)

    def test_zero_window_probe_when_nothing_in_flight(self):
        self.assertEqual(usable_window(1000, 1000, cwnd=2000, rwnd=0), 1)

    def test_zero_window_with_data_in_flight(self):
        self.assertEqual(usable_window(1001, 1000, cwnd=2000, rwnd=0), 0)


class TestSlowStart(unittest.TestCase):

    def test_initial_values(self):
        cc = CubicCC()
        self.assertEqual(cc.cwnd, MSS)
        self.assertEqual(cc.ssthresh, INITIAL_SSTHRESH)
        self.assertEqual(cc.state, "slow_start")

    def test_one_mss_per_new_ack(self):
        cc = CubicCC()
        cc.on_new_ack(0.0)
        self.assertEqual(cc.cwnd, 2 * MSS)

    def test_doubles_every_rtt(self):
        cc = CubicCC()
        for _ in range(3):
            for _ in range(int(cc.cwnd // MSS)):
                cc.on_new_ack(0.0)
        self.assertEqual(cc.cwnd, 8 * MSS)

    def test_switches_to_congestion_avoidance_at_ssthresh(self):
        cc = CubicCC()
        cc.ssthresh = 4 * MSS
        for _ in range(3):
            cc.on_new_ack(0.0)
        self.assertEqual(cc.cwnd, 4 * MSS)
        self.assertEqual(cc.state, "congestion_avoidance")


class TestLossReaction(unittest.TestCase):

    def test_two_dup_acks_do_nothing(self):
        cc = CubicCC()
        cc.cwnd = 20.0 * MSS
        self.assertFalse(cc.on_dup_ack())
        self.assertFalse(cc.on_dup_ack())
        self.assertEqual(cc.cwnd, 20 * MSS)

    def test_triple_dup_ack(self):
        cc = CubicCC()
        cc.cwnd = 100.0 * MSS
        cc.on_dup_ack()
        cc.on_dup_ack()
        self.assertTrue(cc.on_dup_ack())
        self.assertAlmostEqual(cc.w_max, 100)
        self.assertAlmostEqual(cc.ssthresh, 100 * MSS * CUBIC_BETA)
        self.assertAlmostEqual(cc.cwnd, cc.ssthresh)
        self.assertEqual(cc.state, "congestion_avoidance")

    def test_fourth_dup_ack_does_not_cut_again(self):
        cc = CubicCC()
        cc.cwnd = 100.0 * MSS
        for _ in range(3):
            cc.on_dup_ack()
        after_loss = cc.cwnd
        self.assertFalse(cc.on_dup_ack())
        self.assertEqual(cc.cwnd, after_loss)

    def test_new_ack_resets_dup_count(self):
        cc = CubicCC()
        cc.on_dup_ack()
        cc.on_dup_ack()
        cc.on_new_ack(0.0)
        self.assertEqual(cc.dup_ack_count, 0)
        self.assertFalse(cc.on_dup_ack())

    def test_timeout(self):
        cc = CubicCC()
        cc.cwnd = 40.0 * MSS
        cc.on_timeout()
        self.assertEqual(cc.cwnd, MSS)
        self.assertAlmostEqual(cc.ssthresh, 40 * MSS * CUBIC_BETA)
        self.assertEqual(cc.state, "slow_start")

    def test_ssthresh_at_least_two_mss(self):
        cc = CubicCC()
        cc.on_timeout()
        self.assertEqual(cc.ssthresh, 2 * MSS)


def run_rtts(cc, n, now, rtt=0.1):
    for _ in range(n):
        now += rtt
        for _ in range(int(cc.cwnd // cc.mss)):
            cc.on_new_ack(now)
    return now


class TestCubic(unittest.TestCase):

    def after_loss(self):
        cc = CubicCC()
        cc.cwnd = 100.0 * MSS
        cc.ssthresh = 100.0 * MSS
        for _ in range(3):
            cc.on_dup_ack()
        return cc

    def test_k_is_time_to_reach_w_max(self):
        cc = self.after_loss()
        cc.on_new_ack(0.0)
        self.assertAlmostEqual(cc.k, ((100 - 70) / CUBIC_C) ** (1 / 3), places=3)
        self.assertAlmostEqual(cc.w_cubic(cc.k), cc.w_max)

    def test_fast_then_flat_then_fast(self):
        cc = self.after_loss()
        now = run_rtts(cc, 10, 0.0)
        early = cc.cwnd / MSS
        self.assertGreater(early, 70)
        self.assertLess(early, 100)

        now = run_rtts(cc, int(cc.k / 0.1) - 10, now)
        self.assertAlmostEqual(cc.cwnd / MSS, 100, delta=3)

        before = cc.cwnd / MSS
        run_rtts(cc, 30, now)
        self.assertGreater(cc.cwnd / MSS, before + 5)

    def test_cwnd_never_decreases_on_new_ack(self):
        cc = self.after_loss()
        prev = cc.cwnd
        now = 0.0
        for _ in range(300):
            now += 0.02
            cc.on_new_ack(now)
            self.assertGreaterEqual(cc.cwnd, prev)
            prev = cc.cwnd


class TestTcpState(unittest.TestCase):

    def tearDown(self):
        drop_tcp_state(("10.0.0.2", 5000))

    def test_create_get_drop(self):
        key = ("10.0.0.2", 5000)
        state = create_tcp_state(key, peer_rwnd=1234)
        self.assertIs(get_tcp_state(key), state)
        self.assertEqual(state.peer_rwnd, 1234)
        drop_tcp_state(key)
        self.assertIsNone(get_tcp_state(key))

    def test_update_peer_rwnd_creates_state(self):
        key = ("10.0.0.2", 5000)
        update_peer_rwnd(key, 999)
        self.assertEqual(get_tcp_state(key).peer_rwnd, 999)

    def test_usable_window_uses_cwnd_and_peer_rwnd(self):
        state = TcpControlState(peer_rwnd=10000)
        self.assertEqual(state.usable_window(0, 0), MSS)
        state.cc.cwnd = 20.0 * MSS
        self.assertEqual(state.usable_window(0, 0), 10000)


class TestWindowField(unittest.TestCase):

    def window_of(self, header):
        return struct.unpack('!H', header[14:16])[0]

    def test_window_written_into_header(self):
        hdr = rc.build_tcp_packet("10.0.0.1", "10.0.0.2", 1, 2, seq=1, ack_seq=2, ack=1, window=1234)
        self.assertEqual(self.window_of(hdr), 1234)

    def test_default_window_unchanged(self):
        hdr = rc.build_tcp_packet("10.0.0.1", "10.0.0.2", 1, 2, seq=1, ack_seq=2, ack=1)
        self.assertEqual(self.window_of(hdr), 5840)

    def test_window_clamped_to_16_bit(self):
        hdr = rc.build_tcp_packet("10.0.0.1", "10.0.0.2", 1, 2, seq=1, ack_seq=2, ack=1, window=10 ** 6)
        self.assertEqual(self.window_of(hdr), MAX_WINDOW)


if __name__ == "__main__":
    unittest.main()
