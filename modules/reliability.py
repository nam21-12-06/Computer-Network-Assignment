import queue
import time


def check_sequence(client_seq, pkt_seq, payload):
    in_order = pkt_seq == client_seq
    next_ack = client_seq
    if in_order:
        next_ack = (client_seq + len(payload)) % (2 ** 32)
    return in_order, next_ack


def check_ack(waiting, pkt_ack):
    if waiting is None:
        return False
    waiting_seq, waiting_payload = waiting
    expected_ack = (waiting_seq + len(waiting_payload)) % (2 ** 32)
    return pkt_ack == expected_ack


def update_timeout(session, sample_rtt):
    #85, estimate_rrt=(1-alpha)* + alpha = 0.125
    estimated_rtt = 0.875 * session.estimated_rtt + 0.125 * sample_rtt
    #slide 86 Beta = 0.25
    dev_rtt = 0.75 * session.dev_rtt + 0.25 * abs(sample_rtt - estimated_rtt)
    return session._replace(estimated_rtt=estimated_rtt, dev_rtt=dev_rtt, rto=estimated_rtt + 4 * dev_rtt)


def send_reliable(s, conn_info, payload):
    from streaprous_socket import streaprous_raw_connection as raw

    client_key = (conn_info.client_ip, conn_info.client_port)
    with raw.connections_lock:
        session = raw.connections.get(client_key)
        if session is None or session.waiting is not None:
            return -1
        if not payload:
            return 0

    try:
        start_seq = session.server_seq
        waiting = (start_seq, payload)
        for attempt in range(1, 6):
            sent_at = time.monotonic()
            bytes_sent = raw.send_tcp_data_segment(s, conn_info.server_ip, conn_info.client_ip, conn_info.server_port, conn_info.client_port, seq=start_seq, ack_seq=session.client_seq, payload=payload)
            if attempt == 1:
                next_seq = (start_seq + bytes_sent) % (2 ** 32)
                with raw.connections_lock:
                    if client_key in raw.connections:
                        raw.connections[client_key] = session._replace(server_seq=next_seq, waiting=waiting, last_activity=time.time())
                packet_queue = raw.get_packet_queue(client_key)
            deadline = sent_at + session.rto
            try:
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    packet, nouse_addr = packet_queue.get(timeout=remaining)
                    pkt_ack = raw.get_ack(packet)
                    if not check_ack(waiting, pkt_ack):
                        continue
                    with raw.connections_lock:
                        current = raw.connections[client_key]
                        if current.waiting == waiting:
                            if attempt == 1:
                                current = update_timeout(current, time.monotonic() - sent_at)
                            raw.connections[client_key] = current._replace(waiting=None, last_activity=time.time())
                    return bytes_sent
            except queue.Empty:
                continue
        return -1
    except Exception:
        return -1
