import queue
from contextlib import contextmanager
from unittest.mock import patch


class ScriptedQueue:
    def __init__(self, packets):
        self.packets = list(packets)
        self.timeouts = []
        self.now = 0.0
        self.rtt = 0.01
        self.blocked = False

    def get_nowait(self):
        if self.blocked or not self.packets:
            raise queue.Empty()
        item = self.packets.pop(0)
        if isinstance(item, Exception):
            self.blocked = True
            raise item
        self.now += self.rtt
        return item

    def wait(self, timeout):
        self.timeouts.append(timeout)
        self.now += timeout
        self.blocked = False


@contextmanager
def scripted_transport(raw, key, packets):
    state = raw.get_tcp_state(key) or raw.create_tcp_state(key)
    with patch.object(raw, 'get_packet_queue', return_value=packets), \
            patch('time.monotonic', side_effect=lambda: packets.now), \
            patch.object(state.changed, 'wait', side_effect=packets.wait):
        yield state
