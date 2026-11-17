import socket
import struct
from collections import namedtuple

SEQ_MOD = 1 << 32
MAX_WINDOW = 0xFFFF
DEFAULT_MSS = 1460
DEFAULT_PEER_MSS = 536


def seq_add(a, b):
    return (a + b) % SEQ_MOD


def seq_diff(a, b):
    d = (a - b) % SEQ_MOD
    return d - SEQ_MOD if d >= (1 << 31) else d


TcpSegment = namedtuple('TcpSegment', [
    'src_ip', 'src_port', 'dst_port', 'seq', 'ack', 'flags', 'window', 'header', 'payload'
])


def parse_tcp_segment(packet):
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
    i = 20
    while i < len(tcp_header):
        kind = tcp_header[i]
        if kind == 0:
            break
        if kind == 1:
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
