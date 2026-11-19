import time


def check_sequence(client_seq, pkt_seq, payload):
    in_order = (pkt_seq == client_seq)
    next_ack = client_seq
    if in_order:
        next_ack = (client_seq + len(payload))%(2 ** 32)
    return (in_order, next_ack)


def check_ack(waiting, pkt_ack):
    if waiting is None:
        return False
    waiting_seq, waiting_payload = waiting
    expected_ack = (waiting_seq + len(waiting_payload))%(2 ** 32)
    return (pkt_ack == expected_ack)


def update_timeout(session, sample_rtt):
    #85, estimate_rrt=(1-alpha)* + alpha = 0.125
    estimated_rtt = 0.875 * session.estimated_rtt + 0.125 * sample_rtt
    #slide 86 Beta = 0.25
    dev_rtt = 0.75 * session.dev_rtt + 0.25 * abs(sample_rtt - estimated_rtt)
    rto_new = estimated_rtt + 4*dev_rtt
    return session._replace(estimated_rtt=estimated_rtt, dev_rtt=dev_rtt, rto=rto_new)


def send_reliable(s, conn_info, payload):
    from streaprous_socket import streaprous_raw_connection as raw

    client_key = (conn_info.client_ip, conn_info.client_port)
    with raw.connections_lock:
        session = raw.connections.get(client_key)
        if session is None:
            return -1
        if session.waiting is not None:
            return -1
        if not payload:
            return 0

    state = raw.activate_connection(s, client_key, drain=False)
    if state is None or not state.send_lock.acquire(False):
        return -1
    transmitted = False
    try:
        with state.changed:
            if state.closed or state.failed:
                return -1
            with raw.connections_lock:
                session = raw.connections[client_key]
                if session.waiting is not None:
                    return -1
                state.start_seq = session.server_seq
                state.send_size = len(payload)
                raw.connections[client_key] = session._replace(waiting=(session.server_seq, payload))
            state.sent = state.acked = 0
            state.outstanding.clear()
            state.fast_retransmit = False
        probe_deadline = None
        probes = 0
        while True:
            with state.changed:
                if state.closed or state.failed:
                    return -1
                if state.acked == len(payload):
                    return len(payload)
                with raw.connections_lock:
                    session = raw.connections[client_key]
                now = time.monotonic()
                if state.peer_rwnd == 0:
                    interval = max(session.rto, 1.0)
                    if probe_deadline is None:
                        probe_deadline = now + interval
                    if now < probe_deadline:
                        state.changed.wait(probe_deadline - now)
                        continue
                    if probes == 5:
                        raise TimeoutError()
                    probes += 1
                    probe_deadline = now + interval
                    seq = (state.start_seq + state.acked - 1) % (2 ** 32)
                    data = b'\x00'
                else:
                    if probe_deadline is not None:
                        for segment in state.outstanding:
                            segment[2] = now
                        probe_deadline = None
                        probes = 0
                    oldest = state.outstanding[0] if state.outstanding else None
                    expired = oldest is not None and now >= oldest[2] + session.rto
                    if oldest is not None and (state.fast_retransmit or expired):
                        if oldest[3] == 5:
                            raise TimeoutError()
                        if not state.fast_retransmit:
                            state.cc.on_timeout()
                        state.fast_retransmit = False
                        oldest[2] = now
                        oldest[3] += 1
                        seq = (state.start_seq + oldest[0]) % (2 ** 32)
                        data = oldest[1]
                    else:
                        available = state.usable_window(state.sent, state.acked)
                        n = min(state.cc.mss, available, len(payload) - state.sent)
                        if n == 0:
                            timeout = max(0, oldest[2] + session.rto - now) if oldest else None
                            state.changed.wait(timeout)
                            continue
                        data = payload[state.sent:state.sent + n]
                        seq = (state.start_seq + state.sent) % (2 ** 32)
                        state.outstanding.append([state.sent, data, now, 1, False])
                        state.sent += n
                        with raw.connections_lock:
                            current = raw.connections[client_key]
                            raw.connections[client_key] = current._replace(
                                server_seq=(state.start_seq + state.sent) % (2 ** 32),
                                last_activity=time.time())
                window = state.rcv.advertise()
                ack_seq = session.client_seq
            raw.send_tcp_data_segment(s, conn_info.server_ip, conn_info.client_ip,
                                      conn_info.server_port, conn_info.client_port,
                                      seq=seq, ack_seq=ack_seq, payload=data, window=window)
            transmitted = True
            raw.drain_packets(s, client_key)
    except Exception:
        with state.changed:
            state.failed = True
            with raw.connections_lock:
                current = raw.connections.get(client_key)
                if current is not None:
                    raw.connections[client_key] = current._replace(
                        waiting=None, server_seq=current.server_seq if transmitted else state.start_seq)
            state.changed.notify_all()
        return -1
    finally:
        state.send_lock.release()


def acknowledge(state, session, ack, window, duplicate, now):
    previous_window = state.peer_rwnd
    if not state.outstanding:
        if state.send_lock.locked():
            if ack == (state.start_seq + state.acked) % (2 ** 32):
                state.peer_rwnd = window
            return session
        if (session.waiting is None and ack == session.server_seq) or check_ack(session.waiting, ack):
            state.peer_rwnd = window
            return session._replace(waiting=None)
        return session
    advance = (ack - state.start_seq - state.acked) % (2 ** 32)
    if advance > state.sent - state.acked:
        return session
    state.peer_rwnd = window
    if advance == 0:
        if duplicate and window == previous_window and window > 0:
            if state.cc.on_dup_ack():
                state.fast_retransmit = True
        return session
    state.acked += advance
    sample = None
    retransmitted = False
    while state.outstanding and state.outstanding[0][0] < state.acked:
        segment = state.outstanding[0]
        retransmitted = retransmitted or segment[3] > 1 or segment[4]
        sample = now - segment[2]
        consumed = min(len(segment[1]), state.acked - segment[0])
        if consumed == len(segment[1]):
            state.outstanding.popleft()
        else:
            segment[0] += consumed
            segment[1] = segment[1][consumed:]
            segment[4] = True
            break
    if sample is not None and not retransmitted:
        session = update_timeout(session, sample)
    state.fast_retransmit = False
    state.cc.on_new_ack(now)
    if state.acked == state.send_size:
        session = session._replace(waiting=None)
    return session
