import threading

from .flow_control import ReceiveBuffer, MAX_WINDOW, usable_window
from .congestion_control import CubicCC, MSS


class TcpControlState:

    def __init__(self, peer_rwnd=MAX_WINDOW, mss=MSS):
        self.rcv = ReceiveBuffer()
        self.cc = CubicCC(mss)
        self.peer_rwnd = peer_rwnd

    def usable_window(self, last_byte_sent, last_byte_acked):
        return usable_window(last_byte_sent, last_byte_acked, self.cc.cwnd, self.peer_rwnd)


tcp_states = {}
tcp_states_lock = threading.Lock()


def create_tcp_state(client_key, peer_rwnd=MAX_WINDOW):
    state = TcpControlState(peer_rwnd=peer_rwnd)
    with tcp_states_lock:
        tcp_states[client_key] = state
    return state


def get_tcp_state(client_key):
    with tcp_states_lock:
        return tcp_states.get(client_key)


def update_peer_rwnd(client_key, rwnd):
    state = get_tcp_state(client_key) or create_tcp_state(client_key)
    state.peer_rwnd = rwnd
    return state


def drop_tcp_state(client_key):
    with tcp_states_lock:
        tcp_states.pop(client_key, None)
