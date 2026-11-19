RCV_BUFFER = 65535
MAX_WINDOW = 0xFFFF


class ReceiveBuffer:

    def __init__(self, rcv_buffer=RCV_BUFFER):
        self.rcv_buffer = min(rcv_buffer, MAX_WINDOW)
        self.last_byte_rcvd = 0
        self.last_byte_read = 0
        self.data = bytearray()
        self.last_advertised = self.rcv_buffer

    @property
    def rwnd(self):
        return self.rcv_buffer - (self.last_byte_rcvd - self.last_byte_read)

    def on_data(self, payload):
        accepted = payload[:self.rwnd]
        self.data += accepted
        self.last_byte_rcvd += len(accepted)
        return len(accepted)

    def read(self, max_bytes=None):
        n = len(self.data) if max_bytes is None else min(max_bytes, len(self.data))
        out = bytes(self.data[:n])
        del self.data[:n]
        self.last_byte_read += n
        return out

    def advertise(self):
        self.last_advertised = self.rwnd
        return self.last_advertised

    def window_update_needed(self):
        return self.last_advertised == 0 and self.rwnd > 0


def usable_window(last_byte_sent, last_byte_acked, cwnd, rwnd):
    inflight = last_byte_sent - last_byte_acked
    return max(0, min(int(cwnd), rwnd) - inflight)
