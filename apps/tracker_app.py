"""REST routes for tracker, peer discovery, and channel membership."""

from __future__ import annotations

import json

from daemon import StreapRous

from .tracker import TrackerError, TrackerService


app = StreapRous()
tracker = TrackerService()


def _payload(body: object) -> dict:
    if isinstance(body, bytes):
        body = body.decode("utf-8")
    if not isinstance(body, str) or not body.strip():
        return {}
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as error:
        raise TrackerError("request body must be valid JSON") from error
    if not isinstance(payload, dict):
        raise TrackerError("request body must be a JSON object")
    return payload


def _error_response(error: TrackerError) -> tuple[dict, int]:
    return {"error": str(error)}, error.status_code


def _request_ip(headers: dict, payload: dict) -> object:
    """Prefer the connection address injected by HttpAdapter over client JSON."""
    return headers.get("x-remote-ip") or payload.get("ip")


@app.route("/tracker/register", methods=["POST"])
@app.route("/submit-info/", methods=["POST"])
def register_peer(headers: dict, body: object) -> tuple[dict, int]:
    try:
        data = _payload(body)
        peer = tracker.register_peer(
            data.get("peer_id"),
            data.get("user_id"),
            _request_ip(headers, data),
            data.get("listen_port"),
        )
        return {"peer": peer}, 201
    except TrackerError as error:
        return _error_response(error)


@app.route("/tracker/heartbeat", methods=["POST"])
def heartbeat(headers: dict, body: object) -> tuple[dict, int]:
    try:
        data = _payload(body)
        peer = tracker.heartbeat(
            data.get("peer_id"),
            ip=_request_ip(headers, data) if (headers.get("x-remote-ip") or data.get("ip")) else None,
            listen_port=data.get("listen_port"),
        )
        return {"peer": peer}, 200
    except TrackerError as error:
        return _error_response(error)


@app.route("/tracker/peers", methods=["POST"])
@app.route("/get-list/", methods=["GET"])
def list_peers(headers: dict, body: object) -> tuple[dict, int]:
    try:
        data = _payload(body)
        peers = tracker.list_active_peers(exclude_peer_id=data.get("peer_id"))
        return {"peers": peers}, 200
    except TrackerError as error:
        return _error_response(error)


@app.route("/tracker/disconnect", methods=["POST"])
def disconnect_peer(headers: dict, body: object) -> tuple[dict, int]:
    try:
        data = _payload(body)
        peer = tracker.disconnect_peer(data.get("peer_id"))
        return {"peer": peer}, 200
    except TrackerError as error:
        return _error_response(error)


@app.route("/tracker/connect-peer", methods=["POST"])
@app.route("/connect-peer/", methods=["POST"])
def connect_peer(headers: dict, body: object) -> tuple[dict, int]:
    try:
        data = _payload(body)
        target = tracker.connect_peer(data.get("peer_id"), data.get("target_peer_id"))
        return {"target": target}, 200
    except TrackerError as error:
        return _error_response(error)


@app.route("/channels/create", methods=["POST"])
def create_channel(headers: dict, body: object) -> tuple[dict, int]:
    try:
        data = _payload(body)
        channel = tracker.create_channel(data.get("peer_id"), data.get("name"))
        return {"channel": channel}, 201
    except TrackerError as error:
        return _error_response(error)


@app.route("/channels/join", methods=["POST"])
def join_channel(headers: dict, body: object) -> tuple[dict, int]:
    try:
        data = _payload(body)
        channel = tracker.join_channel(data.get("peer_id"), data.get("channel_id"))
        return {"channel": channel}, 200
    except TrackerError as error:
        return _error_response(error)


@app.route("/channels/leave", methods=["POST"])
def leave_channel(headers: dict, body: object) -> tuple[dict, int]:
    try:
        data = _payload(body)
        channel = tracker.leave_channel(data.get("peer_id"), data.get("channel_id"))
        return {"channel": channel}, 200
    except TrackerError as error:
        return _error_response(error)


@app.route("/channels/mine", methods=["POST"])
def list_my_channels(headers: dict, body: object) -> tuple[dict, int]:
    try:
        data = _payload(body)
        channels = tracker.list_channels_for_peer(data.get("peer_id"))
        return {"channels": channels}, 200
    except TrackerError as error:
        return _error_response(error)


@app.route("/channels/members", methods=["POST"])
def list_channel_members(headers: dict, body: object) -> tuple[dict, int]:
    try:
        data = _payload(body)
        members = tracker.list_channel_members(data.get("channel_id"))
        return {"members": members}, 200
    except TrackerError as error:
        return _error_response(error)


def create_tracker_app(ip: str, port: int) -> None:
    """Start the tracker REST application at the supplied address."""
    app.prepare_address(ip, port)
    app.run()
