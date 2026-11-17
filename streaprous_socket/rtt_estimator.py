TCP_INITIAL_RTO = 1.0
TCP_MIN_RTO = 0.2
TCP_MAX_RTO = 60.0


class RttEstimator:

    def __init__(self, initial_rto=None, min_rto=None, max_rto=None):
        self.min_rto = TCP_MIN_RTO if min_rto is None else min_rto
        self.max_rto = TCP_MAX_RTO if max_rto is None else max_rto
        self.srtt = None
        self.rttvar = None
        self.rto = TCP_INITIAL_RTO if initial_rto is None else initial_rto

    def on_sample(self, r):
        if r is None or r < 0:
            return
        if self.srtt is None:
            self.srtt = r
            self.rttvar = r / 2
        else:
            self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - r)
            self.srtt = 0.875 * self.srtt + 0.125 * r
        rto = self.srtt + max(0.001, 4 * self.rttvar)
        self.rto = min(max(rto, self.min_rto), self.max_rto)

    def backoff(self):
        self.rto = min(self.rto * 2, self.max_rto)
