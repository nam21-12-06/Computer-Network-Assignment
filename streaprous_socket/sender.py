import time

from .tcp_common import seq_add, seq_diff
from .rtt_estimator import TCP_MAX_RTO
from .tcp_state import cc_log

TCP_MAX_RETRIES = 8
DUPACK_THRESHOLD = 3

FLAG_FIN_SYN = 0x03
FLAG_RST = 0x04
FLAG_ACK = 0x10


class WindowSender:

    def __init__(self, state, payload, base_seq, transmit):
        self.state = state
        self.payload = payload
        self.total = len(payload)
        self.base = base_seq
        self.transmit = transmit
        self.snd_una = 0
        self.snd_nxt = 0
        self.snd_max = 0
        self.retx_upto = 0
        self.unacked = {}
        self.dupacks = 0
        self.in_recovery = False
        self.recover = 0
        self.consecutive_timeouts = 0
        self.persist_interval = state.rtt.rto
        self.timer_deadline = None
        self.last_window = state.peer_rwnd
        self.reset = False

    @property
    def inflight(self):
        return self.snd_nxt - self.snd_una

    def run(self, next_segment, is_alive):
        cc_log(self.state, f"send start ({self.total} bytes, MSS={self.state.mss})")
        while self.snd_una < self.total:
            if not is_alive():
                return -1
            self.fill_window()
            seg = next_segment(max(0.001, self.timer_deadline - time.time()))
            now = time.time()
            if seg is not None:
                self.on_segment(seg, now)
                if self.reset:
                    return -1
                continue
            if not self.on_timer(now):
                return -1
        cc_log(self.state, "send complete", 0)
        return self.total

    def fill_window(self):
        window = min(self.state.cc.cwnd_bytes, self.state.peer_rwnd)
        while self.snd_nxt < self.total:
            room = window - self.inflight
            length = min(self.state.mss, self.total - self.snd_nxt)
            if room < length:
                if room <= 0 or self.inflight > 0:
                    break
                length = room
            self._send(self.snd_nxt, length)
            self.snd_nxt += length
            self.snd_max = max(self.snd_max, self.snd_nxt)
        if self.timer_deadline is None:
            delay = self.state.rtt.rto if self.snd_nxt > self.snd_una else self.persist_interval
            self.timer_deadline = time.time() + delay

    def on_segment(self, seg, now):
        if seg.flags & FLAG_RST:
            self.reset = True
            return
        if not seg.flags & FLAG_ACK:
            return
        ack_off = seq_diff(seg.ack, self.base)
        if ack_off < self.snd_una or ack_off > self.snd_max:
            return

        old_window = self.last_window
        window_changed = seg.window != old_window
        self.state.peer_rwnd = seg.window
        self.last_window = seg.window

        if ack_off > self.snd_una:
            self._on_new_ack(ack_off, now)
        elif (self.inflight > 0 and self.state.peer_rwnd > 0 and not window_changed
              and not seg.payload and not seg.flags & FLAG_FIN_SYN):
            self._on_dup_ack(now)
        elif window_changed:
            cc_log(self.state, "window update", self.inflight)
            if old_window == 0 and self.state.peer_rwnd > 0:
                self.persist_interval = self.state.rtt.rto
                self.retx_upto = max(self.retx_upto, self.snd_max)
                self.unacked.clear()
                self.snd_nxt = self.snd_una
                self.timer_deadline = None

    def on_timer(self, now):
        if now < self.timer_deadline:
            return True
        if self.state.peer_rwnd == 0:
            self._probe_zero_window(now)
            return True

        self.consecutive_timeouts += 1
        if self.consecutive_timeouts > TCP_MAX_RETRIES:
            return False
        self.state.cc.on_timeout(first=(self.consecutive_timeouts == 1))
        self.state.rtt.backoff()
        self.in_recovery = False
        self.dupacks = 0
        self.recover = self.snd_max
        self.retx_upto = self.snd_max
        self.unacked.clear()
        self.snd_nxt = self.snd_una
        self.timer_deadline = None
        cc_log(self.state, f"RTO timeout #{self.consecutive_timeouts}", 0)
        return True

    def _send(self, off, length):
        self.transmit(seq_add(self.base, off), self.payload[off:off + length])
        self.unacked[off] = (length, time.time())

    def _retransmit_oldest(self, reason):
        length = max(1, min(self.state.mss, self.snd_max - self.snd_una))
        self._send(self.snd_una, length)
        self.retx_upto = max(self.retx_upto, self.snd_una + length)
        cc_log(self.state, reason, self.inflight)

    def _on_new_ack(self, ack_off, now):
        acked = ack_off - self.snd_una
        sample = None
        for off in sorted(self.unacked):
            length, sent_at = self.unacked[off]
            if off + length <= ack_off:
                if off >= self.retx_upto:
                    sample = now - sent_at
                del self.unacked[off]
        if sample is not None:
            self.state.rtt.on_sample(sample)

        self.snd_una = ack_off
        self.snd_nxt = max(self.snd_nxt, self.snd_una)
        self.dupacks = 0
        self.consecutive_timeouts = 0
        self.persist_interval = self.state.rtt.rto

        if self.in_recovery:
            if self.snd_una >= self.recover:
                self.in_recovery = False
                cc_log(self.state, "recovery done", self.inflight)
            else:
                self._retransmit_oldest("partial ACK, retransmit next hole")
        else:
            self.state.cc.on_ack(acked, now, self.state.rtt.srtt)
            cc_log(self.state, f"ACK +{acked}B", self.inflight)

        self.timer_deadline = now + self.state.rtt.rto if self.inflight > 0 else None

    def _on_dup_ack(self, now):
        self.dupacks += 1
        if self.dupacks == DUPACK_THRESHOLD and not self.in_recovery:
            self.state.cc.on_loss(now)
            self.in_recovery = True
            self.recover = self.snd_max
            self._retransmit_oldest("3 dup ACKs, fast retransmit")
            self.timer_deadline = now + self.state.rtt.rto

    def _probe_zero_window(self, now):
        if self.inflight == 0:
            self._send(self.snd_nxt, 1)
            self.snd_nxt += 1
            self.snd_max = max(self.snd_max, self.snd_nxt)
            cc_log(self.state, "zero-window probe", self.inflight)
        else:
            self._retransmit_oldest("zero-window probe (retransmit)")
        self.persist_interval = min(self.persist_interval * 2, TCP_MAX_RTO)
        self.timer_deadline = now + self.persist_interval
