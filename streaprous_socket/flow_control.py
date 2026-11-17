from .tcp_common import SEQ_MOD, MAX_WINDOW, DEFAULT_MSS, seq_add, seq_diff

RECV_BUFFER_SIZE = 65535


class ReceiveBuffer:

    def __init__(self, capacity=None, rcv_nxt=0, mss=DEFAULT_MSS):
        capacity = RECV_BUFFER_SIZE if capacity is None else capacity
        self.capacity = max(0, min(capacity, MAX_WINDOW))
        self.rcv_nxt = rcv_nxt % SEQ_MOD
        self.mss = mss
        self._ready = bytearray()
        self._ooo = {}
        self.last_advertised = self.capacity

    @property
    def buffered(self):
        return len(self._ready)

    @property
    def ooo_bytes(self):
        return sum(len(v) for v in self._ooo.values())

    @property
    def has_gap(self):
        return bool(self._ooo)

    @property
    def rwnd(self):
        return max(0, self.capacity - len(self._ready))

    def receive(self, seq, data):
        if not data:
            return 0
        offset = seq_diff(seq, self.rcv_nxt)
        if offset < 0:
            if -offset >= len(data):
                return 0
            data = data[-offset:]
            offset = 0
        window = self.rwnd
        if offset >= window:
            return 0
        data = data[:window - offset]

        if offset > 0:
            key = seq_add(self.rcv_nxt, offset)
            if len(self._ooo.get(key, b"")) < len(data):
                self._ooo[key] = bytes(data)
            return 0

        self._ready += data
        self.rcv_nxt = seq_add(self.rcv_nxt, len(data))
        return len(data) + self._merge_out_of_order()

    def _merge_out_of_order(self):
        delivered = 0
        progress = True
        while progress and self._ooo:
            progress = False
            for key in list(self._ooo):
                off = seq_diff(key, self.rcv_nxt)
                if off > 0:
                    continue
                seg = self._ooo.pop(key)
                progress = True
                if -off >= len(seg):
                    continue
                tail = seg[-off:] if off < 0 else seg
                self._ready += tail
                self.rcv_nxt = seq_add(self.rcv_nxt, len(tail))
                delivered += len(tail)
        return delivered

    def read(self, max_bytes=None):
        n = len(self._ready) if max_bytes is None else min(max_bytes, len(self._ready))
        data = bytes(self._ready[:n])
        del self._ready[:n]
        return data

    def advertise(self):
        w = min(self.rwnd, MAX_WINDOW)
        self.last_advertised = w
        return w

    def window_update_needed(self):
        threshold = min(self.capacity // 2, self.mss)
        return self.rwnd - self.last_advertised >= max(1, threshold)
