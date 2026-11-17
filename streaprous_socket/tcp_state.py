import threading
import time

from .tcp_common import DEFAULT_MSS, DEFAULT_PEER_MSS, MAX_WINDOW
from .flow_control import ReceiveBuffer
from .congestion_control import CubicCC
from .rtt_estimator import RttEstimator


class TcpControlState:

    def __init__(self, rcv_nxt=0, peer_rwnd=MAX_WINDOW, peer_mss=DEFAULT_PEER_MSS):
        self.mss = max(1, min(DEFAULT_MSS, peer_mss))
        self.rcv = ReceiveBuffer(rcv_nxt=rcv_nxt, mss=self.mss)
        self.cc = CubicCC(mss=self.mss)
        self.rtt = RttEstimator()
        self.peer_rwnd = peer_rwnd
        self.trace = []


tcp_states = {}
tcp_states_lock = threading.Lock()


def create_tcp_state(client_key, rcv_nxt=0, peer_rwnd=MAX_WINDOW, peer_mss=DEFAULT_PEER_MSS):
    state = TcpControlState(rcv_nxt=rcv_nxt, peer_rwnd=peer_rwnd, peer_mss=peer_mss)
    with tcp_states_lock:
        tcp_states[client_key] = state
    return state


def get_tcp_state(client_key):
    with tcp_states_lock:
        return tcp_states.get(client_key)


def drop_tcp_state(client_key):
    with tcp_states_lock:
        tcp_states.pop(client_key, None)


def cc_log(state, event, inflight=0):
    cc = state.cc
    entry = (time.time(), event, round(cc.cwnd, 2), cc.ssthresh, state.peer_rwnd, inflight)
    state.trace.append(entry)
    ssthresh = "inf" if cc.ssthresh == float("inf") else "{:.2f}".format(cc.ssthresh)
    print(f"[Streap_cc] {event}: cwnd={cc.cwnd:.2f} seg ({cc.cwnd_bytes} B), ssthresh={ssthresh}, "
          f"rwnd={state.peer_rwnd}, inflight={inflight}, rto={state.rtt.rto:.3f}s, phase={cc.phase}")
