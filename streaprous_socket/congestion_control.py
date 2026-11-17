from .tcp_common import DEFAULT_MSS

CUBIC_C = 0.4
CUBIC_BETA = 0.7
INITIAL_CWND_SEGMENTS = 10


class CubicCC:

    def __init__(self, mss=DEFAULT_MSS, initial_cwnd=None, c=None, beta=None):
        self.mss = mss
        self.c = CUBIC_C if c is None else c
        self.beta = CUBIC_BETA if beta is None else beta
        self.cwnd = float(INITIAL_CWND_SEGMENTS if initial_cwnd is None else initial_cwnd)
        self.ssthresh = float("inf")
        self.w_max = 0.0
        self.k = 0.0
        self.epoch_start = None
        self.cwnd_epoch = 0.0
        self.w_est = 0.0
        self.loss_events = 0
        self.timeouts = 0

    @property
    def cwnd_bytes(self):
        return int(self.cwnd * self.mss)

    @property
    def in_slow_start(self):
        return self.cwnd < self.ssthresh

    @property
    def phase(self):
        return "slow_start" if self.in_slow_start else "congestion_avoidance"

    def w_cubic(self, t):
        return self.c * (t - self.k) ** 3 + self.w_max

    def on_ack(self, acked_bytes, now, rtt=None):
        if acked_bytes <= 0:
            return
        segs = acked_bytes / self.mss
        if self.in_slow_start:
            room = self.ssthresh - self.cwnd
            if segs <= room:
                self.cwnd += segs
                return
            self.cwnd = self.ssthresh
            segs -= room
        self._congestion_avoidance(segs, now, rtt or 0.0)

    def _congestion_avoidance(self, segs, now, rtt):
        if self.epoch_start is None:
            self.epoch_start = now
            self.cwnd_epoch = self.cwnd
            self.w_est = self.cwnd
            if self.cwnd < self.w_max:
                self.k = ((self.w_max - self.cwnd) / self.c) ** (1.0 / 3.0)
            else:
                self.k = 0.0
                self.w_max = self.cwnd
        t = now - self.epoch_start

        target = self.w_cubic(t + rtt)
        target = max(self.cwnd, min(target, 1.5 * self.cwnd))

        alpha = 1.0 if self.w_est >= self.w_max else 3 * (1 - self.beta) / (1 + self.beta)
        self.w_est += alpha * segs / self.cwnd

        if self.w_cubic(t) < self.w_est:
            self.cwnd = max(self.cwnd, self.w_est)
        else:
            self.cwnd += (target - self.cwnd) / self.cwnd * segs

    def _multiplicative_decrease(self):
        if self.cwnd < self.w_max:
            self.w_max = self.cwnd * (1 + self.beta) / 2
        else:
            self.w_max = self.cwnd
        self.ssthresh = max(self.cwnd * self.beta, 2.0)
        self.epoch_start = None

    def on_loss(self, now=None):
        self._multiplicative_decrease()
        self.cwnd = self.ssthresh
        self.loss_events += 1

    def on_timeout(self, first=True):
        if first:
            self._multiplicative_decrease()
        self.cwnd = 1.0
        self.epoch_start = None
        self.timeouts += 1
