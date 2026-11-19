MSS = 1460
INITIAL_SSTHRESH = 64 * 1024
CUBIC_C = 0.4
CUBIC_BETA = 0.7


class CubicCC:

    def __init__(self, mss=MSS):
        self.mss = mss
        self.cwnd = float(mss)
        self.ssthresh = float(INITIAL_SSTHRESH)
        self.dup_ack_count = 0
        self.w_max = 0.0
        self.k = 0.0
        self.epoch_start = None

    @property
    def state(self):
        return "slow_start" if self.cwnd < self.ssthresh else "congestion_avoidance"

    def w_cubic(self, t):
        return CUBIC_C * (t - self.k) ** 3 + self.w_max

    def on_new_ack(self, now):
        self.dup_ack_count = 0
        if self.cwnd < self.ssthresh:
            self.cwnd += self.mss
            return
        w = self.cwnd / self.mss
        if self.epoch_start is None:
            self.epoch_start = now
            if self.w_max <= w:
                self.w_max = w
                self.k = 0.0
            else:
                self.k = ((self.w_max - w) / CUBIC_C) ** (1.0 / 3.0)
        target = self.w_cubic(now - self.epoch_start)
        if target > w:
            self.cwnd += self.mss * (target - w) / w

    def on_dup_ack(self):
        self.dup_ack_count += 1
        if self.dup_ack_count == 3:
            self._multiplicative_decrease()
            return True
        return False

    def on_timeout(self):
        self._multiplicative_decrease()
        self.cwnd = float(self.mss)
        self.dup_ack_count = 0

    def _multiplicative_decrease(self):
        self.w_max = self.cwnd / self.mss
        self.ssthresh = max(self.cwnd * CUBIC_BETA, 2.0 * self.mss)
        self.cwnd = self.ssthresh
        self.epoch_start = None
