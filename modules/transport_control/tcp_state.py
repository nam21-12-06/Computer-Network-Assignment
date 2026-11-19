import threading
from collections import deque

from .flow_control import ReceiveBuffer, MAX_WINDOW, usable_window
from .congestion_control import CubicCC, MSS


class TcpControlState:

    def __init__(self, peer_rwnd=MAX_WINDOW, mss=MSS):
        self.rcv = ReceiveBuffer()
        self.cc = CubicCC(mss)
        self.peer_rwnd = peer_rwnd
        self.lock = threading.Lock()
        self.changed = threading.Condition(self.lock)
        self.send_lock = threading.Lock()
        self.input_lock = threading.Lock()
        self.packet_queue = None
        self.outstanding = deque()
        self.sent = 0
        self.acked = 0
        self.start_seq = 0
        self.send_size = 0
        self.fast_retransmit = False
        self.active = False
        self.closed = False
        self.failed = False

    def usable_window(self, last_byte_sent, last_byte_acked):
        return usable_window(last_byte_sent, last_byte_acked, self.cc.cwnd, self.peer_rwnd)


tcp_states = {}
tcp_states_lock = threading.Lock()


def create_tcp_state(client_key, peer_rwnd=MAX_WINDOW, **kwargs):
    state = TcpControlState(peer_rwnd=peer_rwnd)
    with tcp_states_lock:
        tcp_states[client_key] = state
    return state


def get_tcp_state(client_key):
    with tcp_states_lock:
        return tcp_states.get(client_key)


def update_peer_rwnd(client_key, rwnd):
    state = get_tcp_state(client_key) or create_tcp_state(client_key)
    with state.changed:
        state.peer_rwnd = rwnd
        state.changed.notify_all()
    return state


def drop_tcp_state(client_key):
    with tcp_states_lock:
        state = tcp_states.pop(client_key, None)
    if state:
        with state.changed:
            state.closed = True
            state.changed.notify_all()
