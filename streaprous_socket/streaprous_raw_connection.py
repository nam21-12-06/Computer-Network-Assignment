#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course,
# and is released under the LICENSE. Please see the LICENSE
# file that should have been included as part of this package.
#
# StreapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#

"""
StreapRous Raw Connection Library
A library for handling TCP connections with raw sockets.
Based on raw_connection_server.py implementation.
Provides connection management, send/receive functionality using raw sockets.
"""

import socket
import struct
import sys
import threading
import time
import queue
from collections import namedtuple

try:
    import netifaces
except ImportError:  # netifaces is only needed for interface lookup; keeps the module importable for unit tests
    netifaces = None

# Import streaprout packet dispatcher functions
from .streaprous_packet_dispatcher import (
    start_packet_dispatcher,
    stop_packet_dispatcher,
    get_packet_queue,
    remove_packet_queue,
    _packet_dispatcher_lock,
    _SYN_KEY,
)


# Connection structure 
# This structure holds connection and its state information
conn_struct = namedtuple('conn_struct', [
    'client_ip',
    'client_port',
    'server_ip',
    'server_port',
    'state',
    'server_seq',
    'client_seq',
    'expected_final_ack',
    'last_activity'
])


# Global table to manage multiple connection
# The mapping include {key: value} as { (client_ip, client_port): conn_struct }
connections = {}
connections_lock = threading.Lock()

# Management structure to serialize the 3-way handshake
# Each hand shake is manage by a unique ID client_key
# including client_ip client_port
handshake_lock = threading.Lock()
handshake_in_progress = {}


# Default raw port listener
DEFAULT_LISTEN_IP = "0.0.0.0"
DEFAULT_LISTEN_PORT = 22239
DEFAULT_INTERFACE_NAME = "lo0"


# ---------------------------------------------------------------------------
# Flow control (rwnd) and congestion control (CUBIC) settings
# ---------------------------------------------------------------------------

SEQ_MOD = 1 << 32              # TCP sequence numbers are 32-bit and wrap around
MAX_WINDOW = 0xFFFF            # 16-bit window field (window scaling is not negotiated)
DEFAULT_MSS = 1460             # Our maximum segment size on Ethernet
DEFAULT_PEER_MSS = 536         # Assumed peer MSS when its SYN carries no MSS option (RFC 9293)
RECV_BUFFER_SIZE = 65535       # Receive buffer capacity per connection (bytes)

# Retransmission timer (RFC 6298)
TCP_INITIAL_RTO = 1.0          # seconds, before any RTT sample
TCP_MIN_RTO = 0.2              # Linux uses 200 ms (RFC 6298 recommends 1 s)
TCP_MAX_RTO = 60.0
TCP_MAX_RETRIES = 8            # consecutive RTOs before the connection is aborted
DUPACK_THRESHOLD = 3           # duplicate ACKs that trigger fast retransmit

# CUBIC (RFC 9438)
CUBIC_C = 0.4
CUBIC_BETA = 0.7               # multiplicative decrease factor
INITIAL_CWND_SEGMENTS = 10     # initial window (RFC 6928)


def seq_add(a, b):
    """Add b to sequence number a modulo 2^32."""
    return (a + b) % SEQ_MOD


def seq_diff(a, b):
    """Signed distance a - b in the 32-bit sequence space."""
    d = (a - b) % SEQ_MOD
    return d - SEQ_MOD if d >= (1 << 31) else d


class RttEstimator:
    """Round-trip time estimator and retransmission timeout (RFC 6298)."""

    def __init__(self, initial_rto=None, min_rto=None, max_rto=None):
        self.min_rto = TCP_MIN_RTO if min_rto is None else min_rto
        self.max_rto = TCP_MAX_RTO if max_rto is None else max_rto
        self.srtt = None
        self.rttvar = None
        self.rto = TCP_INITIAL_RTO if initial_rto is None else initial_rto

    def on_sample(self, r):
        """Update SRTT/RTTVAR with a new RTT measurement r (seconds)."""
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
        """Exponential backoff after a retransmission timeout."""
        self.rto = min(self.rto * 2, self.max_rto)


class ReceiveBuffer:
    """Receiver side of flow control.

    Holds in-order data that the application has not read yet, plus
    out-of-order segments waiting for the gap before them to be filled.
    The advertised receive window is the free space in the buffer:

        rwnd = capacity - bytes buffered but not yet read

    Because rcv_nxt and the buffered byte count grow together, the right
    edge of the window (rcv_nxt + rwnd) never moves left: the window is never shrunk.
    """

    def __init__(self, capacity=None, rcv_nxt=0, mss=DEFAULT_MSS):
        capacity = RECV_BUFFER_SIZE if capacity is None else capacity
        self.capacity = max(0, min(capacity, MAX_WINDOW))
        self.rcv_nxt = rcv_nxt % SEQ_MOD
        self.mss = mss
        self._ready = bytearray()   # in-order bytes not yet read by the application
        self._ooo = {}              # seq -> bytes, out-of-order segments
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
        """Accept a segment. Returns the number of bytes newly delivered in order.

        Duplicate bytes are dropped, bytes beyond the window are cut off,
        and segments that arrive ahead of rcv_nxt are buffered out of order.
        """
        if not data:
            return 0
        offset = seq_diff(seq, self.rcv_nxt)
        if offset < 0:
            if -offset >= len(data):
                return 0                    # entirely old: duplicate
            data = data[-offset:]           # cut the part already received
            offset = 0
        window = self.rwnd
        if offset >= window:
            return 0                        # entirely outside the window
        data = data[:window - offset]       # cut to the right edge of the window

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
                    continue                # already covered
                tail = seg[-off:] if off < 0 else seg
                self._ready += tail
                self.rcv_nxt = seq_add(self.rcv_nxt, len(tail))
                delivered += len(tail)
        return delivered

    def read(self, max_bytes=None):
        """Application reads data. This frees buffer space and opens the window."""
        n = len(self._ready) if max_bytes is None else min(max_bytes, len(self._ready))
        data = bytes(self._ready[:n])
        del self._ready[:n]
        return data

    def advertise(self):
        """Window value to put in the next outgoing segment."""
        w = min(self.rwnd, MAX_WINDOW)
        self.last_advertised = w
        return w

    def window_update_needed(self):
        """Receiver-side silly window syndrome avoidance (RFC 1122 4.2.3.3).

        Send a separate window update only when the window has opened by at
        least min(capacity / 2, MSS) since the last advertisement.
        """
        threshold = min(self.capacity // 2, self.mss)
        return self.rwnd - self.last_advertised >= max(1, threshold)


class CubicCC:
    """CUBIC congestion control (RFC 9438). cwnd and ssthresh are counted in segments.

    - Slow start: cwnd grows by 1 segment for every segment acknowledged.
    - Congestion avoidance: cwnd follows W_cubic(t) = C*(t - K)^3 + W_max,
      where t is the time since the last congestion event and
      K = cbrt((W_max - cwnd_epoch) / C).
    - Reno-friendly region: never grow slower than the Reno estimate W_est.
    - Triple duplicate ACK: W_max = cwnd (with fast convergence),
      ssthresh = cwnd = cwnd * beta.
    - RTO: ssthresh = cwnd * beta, cwnd = 1 segment.
    """

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
        """New data acknowledged."""
        if acked_bytes <= 0:
            return
        segs = acked_bytes / self.mss
        if self.in_slow_start:
            room = self.ssthresh - self.cwnd
            if segs <= room:
                self.cwnd += segs
                return
            self.cwnd = self.ssthresh       # reached ssthresh, the rest counts as congestion avoidance
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

        # Target for one RTT ahead, limited to [cwnd, 1.5 * cwnd]
        target = self.w_cubic(t + rtt)
        target = max(self.cwnd, min(target, 1.5 * self.cwnd))

        # Reno-friendly estimate (alpha becomes 1 once W_est passes W_max)
        alpha = 1.0 if self.w_est >= self.w_max else 3 * (1 - self.beta) / (1 + self.beta)
        self.w_est += alpha * segs / self.cwnd

        if self.w_cubic(t) < self.w_est:
            self.cwnd = max(self.cwnd, self.w_est)                   # Reno-friendly region
        else:
            self.cwnd += (target - self.cwnd) / self.cwnd * segs     # concave / convex region

    def _multiplicative_decrease(self):
        # Fast convergence: give up bandwidth faster when W_max keeps dropping
        if self.cwnd < self.w_max:
            self.w_max = self.cwnd * (1 + self.beta) / 2
        else:
            self.w_max = self.cwnd
        self.ssthresh = max(self.cwnd * self.beta, 2.0)
        self.epoch_start = None

    def on_loss(self, now=None):
        """Loss detected by 3 duplicate ACKs (fast retransmit)."""
        self._multiplicative_decrease()
        self.cwnd = self.ssthresh
        self.loss_events += 1

    def on_timeout(self, first=True):
        """Retransmission timeout. ssthresh is reduced only on the first RTO
        for a segment; repeated RTOs for the same segment keep it (RFC 5681)."""
        if first:
            self._multiplicative_decrease()
        self.cwnd = 1.0
        self.epoch_start = None
        self.timeouts += 1


class TcpControlState:
    """Flow and congestion control state of one connection."""

    def __init__(self, rcv_nxt=0, peer_rwnd=MAX_WINDOW, peer_mss=DEFAULT_PEER_MSS):
        self.mss = max(1, min(DEFAULT_MSS, peer_mss))
        self.rcv = ReceiveBuffer(rcv_nxt=rcv_nxt, mss=self.mss)
        self.cc = CubicCC(mss=self.mss)
        self.rtt = RttEstimator()
        self.peer_rwnd = peer_rwnd      # latest window advertised by the client
        self.trace = []                 # (time, event, cwnd, ssthresh, peer_rwnd, inflight) for plotting


# Global table { (client_ip, client_port): TcpControlState }
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


TcpSegment = namedtuple('TcpSegment', [
    'src_ip', 'src_port', 'dst_port', 'seq', 'ack', 'flags', 'window', 'header', 'payload'
])


def parse_tcp_segment(packet):
    """Parse an IPv4 packet containing TCP. Returns TcpSegment or None."""
    if len(packet) < 20:
        return None
    ihl = (packet[0] & 0x0F) * 4
    tcp = packet[ihl:]
    if len(tcp) < 20:
        return None
    src_port, dst_port, seq, ack, off_res, flags, window, _csum, _urg = struct.unpack('!HHLLBBHHH', tcp[:20])
    hlen = max(20, (off_res >> 4) * 4)
    return TcpSegment(
        src_ip=socket.inet_ntoa(packet[12:16]),
        src_port=src_port, dst_port=dst_port, seq=seq, ack=ack,
        flags=flags, window=window,
        header=tcp[:hlen], payload=tcp[hlen:],
    )


def parse_mss_option(tcp_header):
    """Return the MSS option value from a TCP header (bytes), or None."""
    i = 20
    while i < len(tcp_header):
        kind = tcp_header[i]
        if kind == 0:                   # end of option list
            break
        if kind == 1:                   # NOP
            i += 1
            continue
        if i + 1 >= len(tcp_header):
            break
        length = tcp_header[i + 1]
        if length < 2:
            break
        if kind == 2 and length == 4 and i + 4 <= len(tcp_header):
            return struct.unpack('!H', tcp_header[i + 2:i + 4])[0]
        i += length
    return None


def _cc_log(state, event, inflight=0):
    """Log congestion/flow control state (also stored in state.trace for plotting)."""
    cc = state.cc
    entry = (time.time(), event, round(cc.cwnd, 2), cc.ssthresh, state.peer_rwnd, inflight)
    state.trace.append(entry)
    ssthresh = "inf" if cc.ssthresh == float("inf") else "{:.2f}".format(cc.ssthresh)
    print(f"[Streap_cc] {event}: cwnd={cc.cwnd:.2f} seg ({cc.cwnd_bytes} B), ssthresh={ssthresh}, "
          f"rwnd={state.peer_rwnd}, inflight={inflight}, rto={state.rtt.rto:.3f}s, phase={cc.phase}")

def register_keep_port_alive(listen_ip="0.0.0.0", listen_port=22239, retry_interval=10):
    """
    Periodically register and maintain TCP port.
    Retry registration whenever binding/listening fails.
    Never accept incoming connections.
    
    Args:
        listen_ip: IP address to bind to (default: "0.0.0.0")
        listen_port: Port to bind to (default: 22239)
        retry_interval: Seconds to wait before retrying after failure (default: 10)
    
    Returns:
        threading.Thread: The background thread running the port keep-alive
    """
    def _keep_alive():
        print(f"[Streap_raw]  Start port keep-alive thread for {listen_ip}:{listen_port}")
        attempt = 0
        while True:
            attempt += 1
            standard_sock = None

            try:
                print(f"[Streap_raw] register_keep_port_alive: Attempt #{attempt} - Create socket...")
                standard_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                standard_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

                print(f"[Streap_raw] register_keep_port_alive: Bind to {listen_ip}:{listen_port}...")
                standard_sock.bind((listen_ip, listen_port))
                
                print(f"[Streap_raw] register_keep_port_alive: Listen on port {listen_port}...")
                standard_sock.listen(0)  # No backlog, we won't accept connections
                print(f"[Streap_raw] register_keep_port_alive: Done port {listen_port} registration.")

                # Keep socket alive until an error occurs.
                while True:
                    time.sleep(3600)
            except Exception as e:
                print(f"[Streap_raw] register_keep_port_alive: Fail port registration with error {e}")
            finally:
                if standard_sock:
                    print(f"[Streap_raw] register_keep_port_alive: Close socket...")
                    standard_sock.close()

            print(f"[Streap_raw] register_keep_port_alive: Retry port registration in {retry_interval}s... (attempt #{attempt + 1})")
            time.sleep(retry_interval)
    
    print(f"[Streap_raw] register_keep_port_alive: Starting background thread for port {listen_port}")
    thread = threading.Thread(target=_keep_alive, daemon=True, name=f"port-keep-alive-{listen_port}")
    thread.start()
    print(f"[Streap_raw] register_keep_port_alive: Thread started (name={thread.name})")
    return thread


def checksum(msg):
    """Calculates a checksum and returns computed values.

    Args:
         msg message data
    """
    if len(msg) % 2 == 1:
        msg += b'\x00'
    s = 0
    for i in range(0, len(msg), 2):
        w = struct.unpack('!H', msg[i:i+2])[0]
        s += w
    s = (s >> 16) + (s & 0xffff)
    s += (s >> 16)
    return ~s & 0xffff


def build_tcp_packet(src_ip, dest_ip, src_port, dst_port, seq, ack_seq, syn=0, ack=0, fin=0, window=5840):
    """Stitches a structurally flawless 20-byte TCP header.

    Args:
         src_ip    Source IP address
         dest_ip   Destination IP address
         src_port  Source port
         dst_port  Destination port
         seq       Sequence number
         ack_seq   ACK for sequence
         syn=0     SYN flag
         ack=0     ACK flag
         fin=0     FIN flag
         window    Advertised receive window (rwnd)
    """
    flags = (ack << 4) + (syn << 1) + fin
    offset_res = (5 << 4) + 0  # 20 bytes data offset (5 * 4)
    seq = seq % SEQ_MOD
    ack_seq = ack_seq % SEQ_MOD
    window = max(0, min(int(window), MAX_WINDOW))
    urg_ptr = 0
    draft_checksum = 0

    # Build a draft (20 byte) header with no checksum
    tcp_header_temp = struct.pack('!HHLLBBHHH',
        src_port, dst_port, seq, ack_seq,
        offset_res, flags, window, draft_checksum, urg_ptr
    )
    
    # Build addressing-header
    address_header = struct.pack('!4s4sBBH',
        socket.inet_aton(src_ip), socket.inet_aton(dest_ip),
        0, socket.IPPROTO_TCP, len(tcp_header_temp)
    )
    
    # Calculate mathematical checksum integer
    tcp_checksum = checksum(address_header+ tcp_header_temp)
    
    # Build the final TCP packet
    tcp_header = struct.pack('!HHLLBBHHH',
        src_port, dst_port, seq, ack_seq,
        offset_res, flags, window, tcp_checksum, urg_ptr
    )
    
    return tcp_header


def build_ip_header(src_ip, dest_ip, protocol, payload_len):
    """Builds a structurally flawless 20-byte IP header.

    Args:
         src_ip       Source IP address
         dest_ip      Destination IP address
         protocol     Protocol number
         payload_len  Payload length
    """
    version_ihl = (4 << 4) | 5  # Version 4, IHL 5 (20 bytes)
    tos = 0
    total_len = 20 + payload_len  # IP header + payload

    # TODO: We draft with no-fragment package
    #       Full implementation must include fragment
    #       handling function
    identification = 0x1234
    flags_fragment = 0x4000
    flags_offset = 0 
    ttl = 64
    draft_checksum = 0
    
    # Build draft header with no checksum
    ip_header_temp = struct.pack('!BBHHHBBH4s4s',
        version_ihl, tos, total_len, identification,
        flags_offset, ttl, protocol, draft_checksum,
        socket.inet_aton(src_ip), socket.inet_aton(dest_ip)
    )
    
    # Calculate the checksum
    ip_checksum = checksum(ip_header_temp)
    
    # Build the final IP header
    ip_header = struct.pack('!BBHHHBBH4s4s',
        version_ihl, tos, total_len, identification,
        flags_offset, ttl, protocol, ip_checksum,
        socket.inet_aton(src_ip), socket.inet_aton(dest_ip)
    )
    
    return ip_header


def send_tcp_data_segment(s, src_ip, dest_ip, src_port, dst_port, seq, ack_seq, payload, window=5840, flags=0x18):
    """Assembles and transmits a single TCP segment containing data payload.

    Args:
         src_ip       Source IP address
         dest_ip      Destination IP address
         src_port     Source port
         dst_port     Destination port
         seq          Sequence number
         ack_seq      ACK for sequence
         payload      Payload
         window       Advertised receive window (rwnd)
         flags        TCP flags (default PSH + ACK)

    """

    offset_res = (5 << 4) + 0
    seq = seq % SEQ_MOD
    ack_seq = ack_seq % SEQ_MOD
    window = max(0, min(int(window), MAX_WINDOW))
    urg_ptr = 0
    draft_checksum = 0
    
    # Build draft header with no checksum
    tcp_header_temp = struct.pack('!HHLLBBHHH',
        src_port, dst_port, seq, ack_seq,
        offset_res, flags, window, draft_checksum, urg_ptr
    )
    
    tcp_len = len(tcp_header_temp) + len(payload)
    pseudo = struct.pack('!4s4sBBH',
        socket.inet_aton(src_ip), socket.inet_aton(dest_ip),
        0, socket.IPPROTO_TCP, tcp_len
    )

    # Calculate the checksum    
    tcp_checksum = checksum(pseudo + tcp_header_temp + payload)

    # Build the full TCP packet header    
    tcp_header = struct.pack('!HHLLBBHHH',
        src_port, dst_port, seq, ack_seq,
        offset_res, flags, window, tcp_checksum, urg_ptr
    )

    # Build the full IP packet header     
    ip_header = build_ip_header(src_ip, dest_ip, socket.IPPROTO_TCP, len(tcp_header) + len(payload))

    # Build the full packet header     
    full_packet = ip_header + tcp_header + payload

    # Send packet
    print(f"[Streap_raw] send_tcp_data_segment: seq={seq}, ack_seq={ack_seq}, payload_len={len(payload)}, flags=0x{flags:02x}")
    print(f"[Streap_raw] send_tcp_data_segment: TCP header seq={seq}, ack_seq={ack_seq}, flags=0x{flags:02x}")
    s.sendto(full_packet, (dest_ip, 0))
    
    return len(payload)


def clean_expired_connections(timeout=60):
    """Background thread to clean up stale or timed-out connections.

    Args:
         timeout Time expiration for clean-up working
    """
    while True:
        time.sleep(10)
        now = time.time()
        with connections_lock:
            expired = [k for k, session in connections.items() if now - session.last_activity > timeout]
            for k in expired:
                print(f"[Streap_raw] clean_expired_connections: Session [{k[0]}:{k[1]}] Connection timed out ({timeout}s inactivity). Remove connection.")
                del connections[k]
                drop_tcp_state(k)


def create_raw_socket() -> socket.socket:
    """
    Create a raw socket for TCP protocol. The raw socket requires root privileges.
    It is raw with listen all packet for incoming port.
    
    Returns:
        Raw socket object
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)

        # TODO: port listener need to be post-procesed later
        #       raw_socket ignore the transport protocol deatails

        print(f"[Streap_raw] Create raw socket binding for 0.0.0.0 listening")
        return sock
    except PermissionError:
        print("Error: Root privileges required (sudo).")
        sys.exit(1)
    except OSError as e:
        print(f"Error creating raw socket: {e}")
        sys.exit(1)


def get_interface_ip(interface_name: str = None) -> str:
    """
    Get the IPv4 address of a network interface.
    
    Args:
        interface_name: Network interface name (e.g., 'enp0s8', 'eth0', 'en0')
                        If None, tries to find the default route interface.
    
    Returns:
        IPv4 address string
    """
    try:
        if interface_name:
            addrs = netifaces.ifaddresses(interface_name)
            if netifaces.AF_INET in addrs:
                return addrs[netifaces.AF_INET][0]['addr']
        else:
            # Try to get default route interface
            gateways = netifaces.gateways()
            default_gateway = gateways.get('default', {}).get(netifaces.AF_INET)
            if default_gateway:
                iface = default_gateway[1]
                addrs = netifaces.ifaddresses(iface)
                if netifaces.AF_INET in addrs:
                    return addrs[netifaces.AF_INET][0]['addr']
    except Exception:
        pass
    
    # Fallback: try to get any non-loopback IP
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def accept(s: socket.socket, listen_ip: str, listen_port: int, interface_name: str = None) -> conn_struct:
    """
    Accept a new connection by performing the 3-way handshake using raw sockets.
    Returns a conn_struct tuple for the established connection.
    Handshakes are serialized to ensure only one 3-way handshake is processed at a time.
    
    Args:
        s: Raw socket
        listen_ip: Server IP to listen on (e.g., '0.0.0.0')
        listen_port: Server port to listen on
        interface_name: Network interface name for sending packets (optional)
    
    Returns:
        conn_struct: Established connection information
    """
    # Start packet dispatcher for this port
    start_packet_dispatcher(s, listen_port)
    
    # Get actual source IP for sending packets
    if listen_ip == "0.0.0.0":
        send_ip = get_interface_ip(interface_name)
        print(f"[Streap_raw] accept: listen_ip=0.0.0.0, using send_ip={send_ip} for outgoing packets")
    else:
        send_ip = listen_ip
    
    server_seq = 2000
    client_ip = None
    client_port = None

    print(f"[Streap_raw] accept: Waiting for incoming connections on {listen_ip}:{listen_port} (send_ip={send_ip})...")
    try:
        while True:
            # Get packet from dispatcher queue (for SYN packets)
            syn_q = get_packet_queue(_SYN_KEY)
            try:
                packet, addr = syn_q.get(timeout=1.0)
                print(f"[Streap_raw] accept: Got packet from SYN queue")
            except queue.Empty:
                continue

            # Extract IP Header Length
            ihl = (packet[0] & 0x0F) * 4
            tcp_packet = packet[ihl:]
            
            if len(tcp_packet) < 20:
                continue

            # Unpack the received TCP header
            tcph = struct.unpack('!HHLLBBHHH', tcp_packet[:20])
            src_port, dest_port, pkt_seq, pkt_ack, flags = (
                tcph[0],
                tcph[1],
                tcph[2],
                tcph[3],
                tcph[5],
            )

            # Bypass non-listen port
            if dest_port != listen_port:
                continue

            syn_flag = (flags & 0x02) >> 1
            ack_flag = (flags & 0x10) >> 4

            # Build client_key from client IP and client port
            client_ip = socket.inet_ntoa(packet[12:16])
            client_port = src_port
            client_key = (client_ip, client_port)
            print(f"\n[Streap_raw] accept: Receiv packet from Client ({client_ip}:{client_port}) (Seq={pkt_seq}, SYN={syn_flag}, ACK={ack_flag})")

            # Create queue for each connection associated 
            # with tupple client_key=(client_ip, client_port)
            q = get_packet_queue(client_key)
            print(f"[Streap_raw] accept: Createet per-connection queue for {client_key}")
            with connections_lock:
                # Declare initialized session if not available
                if client_key not in connections:
                    connections[client_key] = conn_struct(
                        client_ip=client_ip,
                        client_port=client_port,
                        server_ip=send_ip,
                        server_port=listen_port,
                        state="LISTEN",
                        server_seq=2000,
                        client_seq=0,
                        expected_final_ack=0,
                        last_activity=time.time()
                    )
                session = connections[client_key]

                        
            # Perform 3 handshake step
            # STEP 1: Obtain client SYN 
            if syn_flag == 1 and ack_flag == 0:
                print(f"\nHandshake1: Received SYN from Client ({client_ip}:{client_port}) (Seq={pkt_seq})")

                # Quick duplicate handshake check (fast, no heavy operations)
                with handshake_lock:
                    if handshake_in_progress.get(client_key, False):
                        print(f"[Streap_raw] accept: Handshake already in progress for {client_ip}:{client_port}, dropping SYN")
                        continue
                    handshake_in_progress[client_key] = True

                # STEP 2: Reply SYN-ACK immediately
                # Build TCP packet for SYN-ACK
                next_ack = seq_add(pkt_seq, 1)

                # Flow/congestion control state: peer window and MSS come from the SYN
                syn_seg = parse_tcp_segment(packet)
                peer_mss = parse_mss_option(syn_seg.header) if syn_seg else None
                tcp_state = create_tcp_state(client_key, rcv_nxt=next_ack,
                                             peer_rwnd=tcph[6],
                                             peer_mss=peer_mss or DEFAULT_PEER_MSS)
                print(f"[Streap_raw] accept: peer rwnd={tcph[6]}, peer MSS={peer_mss}, using MSS={tcp_state.mss}")

                syn_ack_packet = build_tcp_packet(send_ip, client_ip, listen_port, client_port,
                                                  seq=server_seq, ack_seq=next_ack, syn=1, ack=1,
                                                  window=tcp_state.rcv.advertise())
                ip_header = build_ip_header(send_ip, client_ip, socket.IPPROTO_TCP, len(syn_ack_packet))
                full_packet = ip_header + syn_ack_packet

                # Send SYN-ACK packet
                print(f"[Streap_raw] accept: Sending SYN-ACK to {client_ip}:{client_port} from {send_ip}:{listen_port}")
                print(f"[Streap_raw] accept: SYN-ACK packet size: {len(full_packet)} bytes (IP={len(ip_header)}, TCP={len(syn_ack_packet)})")
                bytes_sent = s.sendto(full_packet, (client_ip, 0))
                syn_ack_sent_at = time.time()
                print(f"Handshake2: Sent SYN-ACK to Client (Seq={server_seq}, Ack={next_ack}, bytes_sent={bytes_sent})")

                # Register connection to the associated queue for each connection associated 
                # with tupple client_key=(client_ip, client_port)
                with connections_lock:
                    connections[client_key] = conn_struct(
                        client_ip=client_ip,
                        client_port=client_port,
                        server_ip=send_ip,  # Use actual sending IP
                        server_port=listen_port,
                        state="SYN_RCVD",
                        server_seq=server_seq,
                        client_seq=pkt_seq,
                        expected_final_ack=server_seq + 1,
                        last_activity=time.time()
                    )
                    session = connections[client_key]

                # STEP 3: Wait for client FIN-ACK
                # The final ACK will arrive on the per-connection queue, not the SYN queue
                deadline = time.time() + 10.0
                while time.time() < deadline:
                    try:
                        packet, addr = q.get(timeout=0.5)
                        print(f"[Streap_raw] accept: Got packet from per-connection queue for {client_ip}:{client_port}")
                    except queue.Empty:
                        print(f"[Streap_raw] accept: Per-connection queue empty for {client_ip}:{client_port}, waiting...")
                        continue

                    # Extract IP Header Length
                    ihl = (packet[0] & 0x0F) * 4
                    tcp_packet = packet[ihl:]
                    
                    if len(tcp_packet) < 20:
                        continue

                    # Unpack the received TCP header
                    tcph = struct.unpack('!HHLLBBHHH', tcp_packet[:20])
                    src_port, dest_port, pkt_seq, pkt_ack, flags = (
                        tcph[0],
                        tcph[1],
                        tcph[2],
                        tcph[3],
                        tcph[5],
                    )

                    syn_flag = (flags & 0x02) >> 1
                    ack_flag = (flags & 0x10) >> 4
                    print(f"[Streap_raw] accept: Received packet in handshake wait - SYN={syn_flag}, ACK={ack_flag}, Seq={pkt_seq}, Ack={pkt_ack}")

                    # Check if this is the final ACK (ACK only, no SYN)
                    if syn_flag == 0 and ack_flag == 1:
                        print(f"[Streap_raw] accept: handshake3 Received FIN-ACK from Client (Seq={pkt_seq}, Ack={pkt_ack})")

                        with connections_lock:
                            if session.state != "SYN_RCVD":
                                print(f"[Streap_raw] accept: Non handshake state for {client_ip}:{client_port}")
                                break

                            # Update connection to ESTABLISHED
                            connections[client_key] = conn_struct(
                                client_ip=client_ip,
                                client_port=client_port,
                                server_ip=send_ip,
                                server_port=listen_port,
                                state="ESTABLISHED",
                                server_seq=pkt_ack,
                                client_seq=pkt_seq,
                                expected_final_ack=session.expected_final_ack,
                                last_activity=time.time()
                            )
                            session = connections[client_key]

                        # Window from the final ACK, and first RTT sample (SYN-ACK -> ACK)
                        tcp_state.peer_rwnd = tcph[6]
                        tcp_state.rtt.on_sample(time.time() - syn_ack_sent_at)
                        _cc_log(tcp_state, "established")

                        # Clear handshake in progress flag
                        with handshake_lock:
                            handshake_in_progress[client_key] = False

                        print(f"[Streap_raw] Etablished handshak with connection: ({client_ip}:{client_port}) ESTABLISHED")

                        # Return the established connection info
                        return session

                # Fallback of the handshake timeout
                print(f"[Streap_raw] Handshake timeout for {client_ip}:{client_port}")
                with handshake_lock:
                    handshake_in_progress[client_key] = False
                remove_packet_queue(client_key)
                with connections_lock:
                    connections.pop(client_key, None)
                drop_tcp_state(client_key)
                continue

            # STEP 3 Exception handling: Capture FIN ACK on SYN queue ---
            if session.state == "SYN_RCVD" and syn_flag == 0 and ack_flag == 1:
                print(f"[Streap_raw] accept: handshake3 received Final ACK from Client on SYN queue (Seq={pkt_seq}, Ack={pkt_ack})")
                print(f"[Streap_raw] accept: Fallback - got final ACK on SYN queue for {client_ip}:{client_port}")

                with connections_lock:
                    if session.state != "SYN_RCVD":
                        print(f"[Streap_raw] accept: Non handshake state for {client_ip}:{client_port}")
                        continue

                    # Update connection to ESTABLISHED
                    connections[client_key] = conn_struct(
                        client_ip=client_ip,
                        client_port=client_port,
                        server_ip=send_ip,
                        server_port=listen_port,
                        state="ESTABLISHED",
                        server_seq=pkt_ack,
                        client_seq=pkt_seq,
                        expected_final_ack=session.expected_final_ack,
                        last_activity=time.time()
                    )
                    session = connections[client_key]

                fallback_state = get_tcp_state(client_key)
                if fallback_state is None:
                    fallback_state = create_tcp_state(client_key, rcv_nxt=pkt_seq, peer_rwnd=tcph[6])
                fallback_state.peer_rwnd = tcph[6]

                # Clear handshake in progress flag
                with handshake_lock:
                    handshake_in_progress[client_key] = False

                print(f"[Streap_raw] Etablished handshak with connection: ({client_ip}:{client_port}) ESTABLISHED")
                
                # Return the established connection info
                return session

    except KeyboardInterrupt:
        print("\n[-] Server stopped.")
        # Clear handshake flags on interrupt
        with handshake_lock:
            handshake_in_progress.clear()
        raise


def _state_for(client_key, conn_info):
    """Return the flow/congestion control state of a connection, creating it if needed."""
    state = get_tcp_state(client_key)
    if state is None:
        # Connection registered without accept(): client_seq is the next expected client byte
        state = create_tcp_state(client_key, rcv_nxt=conn_info.client_seq)
    return state


def _send_ack(s, session, state, client_ip, client_port, listen_port, reason="ACK"):
    """Send a pure ACK carrying the cumulative ACK (rcv_nxt) and the current rwnd."""
    window = state.rcv.advertise()
    ack_packet = build_tcp_packet(session.server_ip, client_ip, listen_port, client_port,
                                  seq=session.server_seq, ack_seq=state.rcv.rcv_nxt,
                                  ack=1, window=window)
    ip_header = build_ip_header(session.server_ip, client_ip, socket.IPPROTO_TCP, len(ack_packet))
    s.sendto(ip_header + ack_packet, (client_ip, 0))
    print(f"[Streap_raw] Sent {reason} (Ack={state.rcv.rcv_nxt}, rwnd={window})")


def run_conn_recv(s: socket.socket, conn_info: conn_struct) -> bytes:
    """
    Receive data on an established connection using raw sockets.
    This function handles incoming packets for a specific connection by reading
    from the packet dispatcher's per-connection queue.

    Flow control (receiver side):
      - Segments go through the connection's ReceiveBuffer: duplicates are
        dropped, out-of-order segments are buffered until the gap is filled,
        and data beyond the advertised window is cut off.
      - Every data segment is ACKed immediately with the cumulative ACK
        (rcv_nxt) and the current rwnd. Out-of-order segments therefore
        produce duplicate ACKs, which the sender uses for fast retransmit.
      - When the application reads the buffer and the window opens enough,
        a window update is sent.
      - The window advertised by the client is stored for the sender side.

    Args:
        s: raw socket
        conn_info: conn_struct tuple for the connection

    Returns:
        bytes: The received in-order data, or empty bytes if connection closed
    """
    client_ip = conn_info.client_ip
    client_port = conn_info.client_port
    client_key = (client_ip, client_port)
    listen_port = conn_info.server_port

    # Get the packet queue for this connection from the dispatcher
    packet_queue = get_packet_queue(client_key)
    state = _state_for(client_key, conn_info)

    print(f"[Streap_raw] run_conn_recv: Starting receive loop for {client_ip}:{client_port} (waiting for data...)")

    try:
        while True:
            # Get packet from the dispatcher queue instead of reading directly from socket
            try:
                packet, _addr = packet_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            seg = parse_tcp_segment(packet)
            if seg is None:
                continue

            # Bypass non-listen port
            if seg.dst_port != listen_port:
                continue

            # Check if this packet is for our connection
            if seg.src_port != client_port:
                continue

            flags = seg.flags
            syn_flag = (flags & 0x02) >> 1
            ack_flag = (flags & 0x10) >> 4
            psh_flag = (flags & 0x08) >> 3
            fin_flag = (flags & 0x01)
            rst_flag = (flags & 0x04) >> 2

            print(f"[Streap_raw] run_conn_recv: Received packet from {client_ip}:{client_port} (SYN={syn_flag}, ACK={ack_flag}, PSH={psh_flag}, FIN={fin_flag}, RST={rst_flag}, Seq={seg.seq}, Ack={seg.ack}, Win={seg.window})")

            with connections_lock:
                if client_key not in connections:
                    continue
                session = connections[client_key]

            # Handle RST (connection reset)
            if rst_flag:
                print(f"[Streap_raw] Received RST from {client_ip}:{client_port}")
                with connections_lock:
                    if client_key in connections:
                        del connections[client_key]
                # Clean up packet queue
                remove_packet_queue(client_key)
                drop_tcp_state(client_key)
                return b""

            # Flow control: remember the window advertised by the client
            if ack_flag:
                state.peer_rwnd = seg.window

            app_payload = b"" if syn_flag else seg.payload
            delivered = 0
            if app_payload:
                delivered = state.rcv.receive(seg.seq, app_payload)
                print(f"[Streap_raw] Received {len(app_payload)} bytes of data from {client_ip}:{client_port} "
                      f"(in-order delivered={delivered}, buffered={state.rcv.buffered}, "
                      f"out-of-order={state.rcv.ooo_bytes}, rwnd={state.rcv.rwnd})")
                print(f"[Streap_raw] Received payload: {app_payload[:100]}...")

            # Handle FIN (connection close)
            if fin_flag:
                print(f"[Streap_raw] Received FIN from {client_ip}:{client_port}")
                fin_seq = seq_add(seg.seq, len(app_payload))
                if fin_seq == state.rcv.rcv_nxt:
                    state.rcv.rcv_nxt = seq_add(fin_seq, 1)   # FIN consumes one sequence number
                # Send ACK for FIN
                _send_ack(s, session, state, client_ip, client_port, listen_port, reason="ACK for FIN")

                with connections_lock:
                    if client_key in connections:
                        del connections[client_key]
                # Clean up packet queue
                remove_packet_queue(client_key)
                drop_tcp_state(client_key)
                return b""

            if app_payload:
                # ACK immediately (cumulative ACK = rcv_nxt)
                reason = "ACK for data" if delivered else "duplicate ACK (out-of-order/duplicate segment)"
                _send_ack(s, session, state, client_ip, client_port, listen_port, reason=reason)

                # The application consumes the in-order data
                data = state.rcv.read()
                with connections_lock:
                    if client_key in connections:
                        connections[client_key] = connections[client_key]._replace(
                            client_seq=state.rcv.rcv_nxt,
                            last_activity=time.time()
                        )
                if data:
                    # Buffer space freed: tell the client the window has opened
                    if state.rcv.window_update_needed():
                        _send_ack(s, session, state, client_ip, client_port, listen_port, reason="window update")
                    return data
                continue

            # Handle pure ACK (no data)
            if ack_flag and not syn_flag:
                print(f"[Streap_raw] Received ACK from {client_ip}:{client_port} (Ack={seg.ack})")
                with connections_lock:
                    if client_key in connections:
                        cur = connections[client_key]
                        new_seq = seg.ack if seq_diff(seg.ack, cur.server_seq) > 0 else cur.server_seq
                        connections[client_key] = cur._replace(
                            server_seq=new_seq,
                            last_activity=time.time()
                        )

    except KeyboardInterrupt:
        print("\n[Streap_raw] Receive loop interrupted.")
        return b""
    except Exception as e:
        print(f"[Streap_raw] Error in run_conn_recv: {e}")
        return b""


def run_conn_send(s: socket.socket, conn_info: conn_struct, payload: bytes) -> int:
    """
    Send data reliably on an established connection using raw sockets,
    with flow control and CUBIC congestion control.

    - The payload is split into MSS-sized segments.
    - At most min(cwnd, rwnd) bytes are in flight (sent but not ACKed).
      rwnd is the window advertised by the client (flow control), cwnd comes
      from CubicCC (congestion control).
    - New ACK: cwnd grows (slow start or CUBIC), RTT is sampled (Karn's
      algorithm: never from retransmitted segments) and the RTO is updated.
    - 3 duplicate ACKs: fast retransmit, cwnd = ssthresh = cwnd * beta.
      Partial ACKs during recovery retransmit the next hole (NewReno).
    - RTO: cwnd = 1 MSS, RTO doubles, sending restarts from the oldest
      unacknowledged byte. The connection is aborted after TCP_MAX_RETRIES.
    - Zero window: the persist timer sends 1-byte window probes, without any
      congestion reaction.

    Args:
        s: Raw socket
        conn_info: conn_struct tuple for the connection
        payload: Bytes payload to send

    Returns:
        int: Number of bytes sent and acknowledged, or -1 on error
    """
    client_ip = conn_info.client_ip
    client_port = conn_info.client_port
    client_key = (client_ip, client_port)
    listen_ip = conn_info.server_ip
    listen_port = conn_info.server_port

    with connections_lock:
        if client_key not in connections:
            print(f"[Streap_raw] run_conn_send Connection not found for {client_ip}:{client_port}")
            return -1
        session = connections[client_key]

    payload = bytes(payload or b"")
    total = len(payload)
    if total == 0:
        return 0

    state = _state_for(client_key, session)
    cc, rtt, mss = state.cc, state.rtt, state.mss
    q = get_packet_queue(client_key)
    base = session.server_seq

    # All positions below are byte offsets inside payload
    snd_una = 0            # oldest unacknowledged byte
    snd_nxt = 0            # next byte to send
    snd_max = 0            # highest byte ever sent
    retx_upto = 0          # bytes below this offset were retransmitted (no RTT sample, Karn)
    unacked = {}           # offset -> (length, send_time)
    dupacks = 0
    in_recovery = False
    recover = 0
    consecutive_timeouts = 0
    persist_interval = rtt.rto
    timer_deadline = None
    last_window = state.peer_rwnd

    def transmit(off, length):
        send_tcp_data_segment(
            s, listen_ip, client_ip, listen_port, client_port,
            seq=seq_add(base, off), ack_seq=state.rcv.rcv_nxt,
            payload=payload[off:off + length], window=state.rcv.advertise()
        )
        unacked[off] = (length, time.time())

    def retransmit_oldest(reason):
        nonlocal retx_upto
        length = max(1, min(mss, snd_max - snd_una))
        transmit(snd_una, length)
        retx_upto = max(retx_upto, snd_una + length)
        _cc_log(state, reason, snd_nxt - snd_una)

    def abort(reason):
        print(f"[Streap_raw] run_conn_send: abort {client_ip}:{client_port}: {reason}")
        with connections_lock:
            if client_key in connections:
                connections[client_key] = connections[client_key]._replace(
                    server_seq=seq_add(base, snd_max))
        return -1

    _cc_log(state, f"send start ({total} bytes, MSS={mss})")

    try:
        while snd_una < total:
            with connections_lock:
                if client_key not in connections:
                    print(f"[Streap_raw] run_conn_send Connection closed for {client_ip}:{client_port}")
                    return -1

            # 1. Send new segments while min(cwnd, rwnd) allows it
            window = min(cc.cwnd_bytes, state.peer_rwnd)
            while snd_nxt < total:
                inflight = snd_nxt - snd_una
                room = window - inflight
                length = min(mss, total - snd_nxt)
                if room < length:
                    # Sender-side SWS avoidance: a short segment only when nothing is in flight
                    if room <= 0 or inflight > 0:
                        break
                    length = room
                transmit(snd_nxt, length)
                snd_nxt += length
                snd_max = max(snd_max, snd_nxt)

            now = time.time()
            if timer_deadline is None:
                if snd_nxt > snd_una:
                    timer_deadline = now + rtt.rto              # retransmission timer
                else:
                    timer_deadline = now + persist_interval     # zero window: persist timer

            # 2. Wait for an ACK or for the timer to expire
            try:
                packet, _addr = q.get(timeout=max(0.001, timer_deadline - time.time()))
            except queue.Empty:
                packet = None
            now = time.time()

            if packet is not None:
                seg = parse_tcp_segment(packet)
                if seg is None or seg.dst_port != listen_port or seg.src_port != client_port:
                    continue
                if seg.flags & 0x04:   # RST
                    print(f"[Streap_raw] run_conn_send: Received RST from {client_ip}:{client_port}")
                    with connections_lock:
                        connections.pop(client_key, None)
                    remove_packet_queue(client_key)
                    drop_tcp_state(client_key)
                    return -1
                if not seg.flags & 0x10:   # no ACK flag
                    continue

                ack_off = seq_diff(seg.ack, base)
                if ack_off < snd_una or ack_off > snd_max:
                    continue               # old or invalid ACK

                old_window = last_window
                window_changed = seg.window != old_window
                state.peer_rwnd = seg.window
                last_window = seg.window

                if ack_off > snd_una:
                    # New data acknowledged
                    acked = ack_off - snd_una
                    sample = None
                    for off in sorted(unacked):
                        length, sent_at = unacked[off]
                        if off + length <= ack_off:
                            if off >= retx_upto:
                                sample = now - sent_at
                            del unacked[off]
                    if sample is not None:
                        rtt.on_sample(sample)

                    snd_una = ack_off
                    if snd_nxt < snd_una:
                        snd_nxt = snd_una
                    dupacks = 0
                    consecutive_timeouts = 0
                    persist_interval = rtt.rto

                    if in_recovery:
                        if snd_una >= recover:
                            in_recovery = False
                            _cc_log(state, "recovery done", snd_nxt - snd_una)
                        else:
                            retransmit_oldest("partial ACK, retransmit next hole")
                    else:
                        cc.on_ack(acked, now, rtt.srtt)
                        _cc_log(state, f"ACK +{acked}B", snd_nxt - snd_una)

                    timer_deadline = now + rtt.rto if snd_nxt > snd_una else None
                    with connections_lock:
                        if client_key in connections:
                            connections[client_key] = connections[client_key]._replace(last_activity=now)

                elif (snd_nxt > snd_una and state.peer_rwnd > 0 and not window_changed
                      and not seg.payload and not seg.flags & 0x03):
                    # Duplicate ACK (RFC 5681: same ACK, no data, window unchanged)
                    dupacks += 1
                    if dupacks == DUPACK_THRESHOLD and not in_recovery:
                        cc.on_loss(now)
                        in_recovery = True
                        recover = snd_max
                        retransmit_oldest("3 dup ACKs, fast retransmit")
                        timer_deadline = now + rtt.rto

                elif window_changed:
                    _cc_log(state, "window update", snd_nxt - snd_una)
                    if old_window == 0 and state.peer_rwnd > 0:
                        # Window reopened: resend from the oldest byte (a rejected probe included)
                        persist_interval = rtt.rto
                        retx_upto = max(retx_upto, snd_max)
                        unacked.clear()
                        snd_nxt = snd_una
                        timer_deadline = None
                continue

            if now < timer_deadline:
                continue

            # 3. Timer expired
            if state.peer_rwnd == 0:
                # Persist timer: probe the zero window, no congestion reaction
                if snd_nxt == snd_una:
                    transmit(snd_nxt, 1)
                    snd_nxt += 1
                    snd_max = max(snd_max, snd_nxt)
                    _cc_log(state, "zero-window probe", snd_nxt - snd_una)
                else:
                    retransmit_oldest("zero-window probe (retransmit)")
                persist_interval = min(persist_interval * 2, TCP_MAX_RTO)
                timer_deadline = now + persist_interval
                continue

            consecutive_timeouts += 1
            if consecutive_timeouts > TCP_MAX_RETRIES:
                return abort(f"{TCP_MAX_RETRIES} retransmission timeouts")
            cc.on_timeout(first=(consecutive_timeouts == 1))
            rtt.backoff()
            in_recovery = False
            dupacks = 0
            recover = snd_max
            retx_upto = snd_max
            unacked.clear()
            snd_nxt = snd_una      # go back and resend from the oldest unacknowledged byte
            timer_deadline = None
            _cc_log(state, f"RTO timeout #{consecutive_timeouts}", 0)

        with connections_lock:
            if client_key in connections:
                connections[client_key] = connections[client_key]._replace(
                    server_seq=seq_add(base, total),
                    client_seq=state.rcv.rcv_nxt,
                    last_activity=time.time()
                )
        _cc_log(state, "send complete", 0)
        print(f"[Streap_raw] run_conn_send Sent {total} bytes to {client_ip}:{client_port}")
        return total

    except Exception as e:
        print(f"[Streap_raw] run_conn_send Error in run_conn_send: {e}")
        return -1


def close_connection(s: socket.socket, conn_info: conn_struct):
    """Close a specific connection by sending FIN and removing from connection table."""
    client_ip = conn_info.client_ip
    client_port = conn_info.client_port
    client_key = (client_ip, client_port)
    listen_ip = conn_info.server_ip
    listen_port = conn_info.server_port
    state = get_tcp_state(client_key)

    with connections_lock:
        if client_key in connections:
            session = connections[client_key]
            ack_seq = state.rcv.rcv_nxt if state else session.client_seq
            window = state.rcv.advertise() if state else 5840
            # Send FIN packet
            fin_packet = build_tcp_packet(listen_ip, client_ip, listen_port, client_port,
                                          seq=session.server_seq, ack_seq=ack_seq, ack=1, fin=1,
                                          window=window)
            ip_header = build_ip_header(listen_ip, client_ip, socket.IPPROTO_TCP, len(fin_packet))
            s.sendto(ip_header + fin_packet, (client_ip, 0))
            print(f"[Streap_raw] close_connection: Send FIN to {client_ip}:{client_port}")
            del connections[client_key]

    # Clean up packet queue
    remove_packet_queue(client_key)
    drop_tcp_state(client_key)


def close_all_connections():
    """Close all connections and clean up."""
    with connections_lock:
        connections.clear()
