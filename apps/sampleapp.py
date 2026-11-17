#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course,
# and is released under the "MIT License Agreement". Please see the LICENSE
# file that should have been included as part of this package.
#
# StreapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#

"""
apps.sampleapp
~~~~~~~~~~~~~~~~~

RESTful TCP WebApp implementation using the StreapRous framework.

This module implements the Application Layer (Chapter 2) for a Hybrid
Architecture (Client-Server + P2P):
  1. Client-Server Phase: Authentication with HTTP Cookies (RFC 6265) and
     centralized Peer Tracker / Discovery.
  2. P2P Phase: Direct peer-to-peer message dispatching over TCP sockets.
"""

import json
import os
import socket
import threading
import time
import uuid
from urllib.parse import parse_qs

from daemon import StreapRous

# Initialize the StreapRous application router (Client-Server separation)
app = StreapRous()

# Base directory for serving Chat UI static HTML assets
WWW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "www")

# ---------------------------------------------------------------------------
# Fallback In-Memory State (used when teammate modules are not yet integrated)
# ---------------------------------------------------------------------------
_STATE_LOCK = threading.Lock()
_ACTIVE_SESSIONS = {}    # session_id -> username
_PEER_REGISTRY = {}      # (ip, port) -> {"ip": str, "port": int, "username": str}
_CHANNELS = {            # channel_name -> {"name": str, "creator": str, "members": set()}
    "general": {"name": "general", "creator": "system", "members": set()}
}
_CHANNEL_MESSAGES = {    # channel_name -> list of immutable message dicts (append-only)
    "general": []
}


# ---------------------------------------------------------------------------
# HTTP Helper Functions (Status Codes, MIME Types, Headers & Cookies)
# ---------------------------------------------------------------------------

def build_json_response(status_code, reason, payload, extra_headers=None):
    """
    Construct a complete HTTP/1.1 response with JSON payload and MIME metadata.

    Implements Chapter 2 HTTP Response Format & MIME Types and the REST
    Self-descriptive Messages constraint.

    :param status_code (int): HTTP status code (e.g., 200, 400, 401).
    :param reason (str): HTTP reason phrase (e.g., "OK", "Bad Request").
    :param payload (dict | list): JSON-serializable response body.
    :param extra_headers (dict, optional): Additional HTTP headers (e.g., Set-Cookie).
    :rtype: bytes
    """
    body_bytes = json.dumps(payload).encode("utf-8")

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Content-Length": str(len(body_bytes)),
        "Connection": "close",
    }
    if extra_headers:
        headers.update(extra_headers)

    status_line = f"HTTP/1.1 {status_code} {reason}\r\n"
    header_lines = "".join(f"{key}: {val}\r\n" for key, val in headers.items())
    return (status_line + header_lines + "\r\n").encode("utf-8") + body_bytes


def build_html_response(status_code, reason, html_content):
    """
    Construct a complete HTTP/1.1 response for serving Chat UI HTML pages.

    :param status_code (int): HTTP status code (e.g., 200, 404).
    :param reason (str): HTTP reason phrase (e.g., "OK", "Not Found").
    :param html_content (str | bytes): HTML document body.
    :rtype: bytes
    """
    body_bytes = html_content.encode("utf-8") if isinstance(html_content, str) else html_content
    headers = {
        "Content-Type": "text/html; charset=utf-8",
        "Content-Length": str(len(body_bytes)),
        "Connection": "close",
    }
    status_line = f"HTTP/1.1 {status_code} {reason}\r\n"
    header_lines = "".join(f"{key}: {val}\r\n" for key, val in headers.items())
    return (status_line + header_lines + "\r\n").encode("utf-8") + body_bytes


def extract_header_value(headers, target_name):
    """
    Extract a single header value (case-insensitive) from dict or raw header string.

    :param headers (dict | str): Incoming request headers.
    :param target_name (str): Header name to look up.
    :rtype: str | None
    """
    target_lower = target_name.lower()
    if isinstance(headers, dict):
        for key, val in headers.items():
            if key.lower() == target_lower:
                return val
    elif isinstance(headers, str):
        prefix = f"{target_lower}:"
        for line in headers.splitlines():
            if line.lower().startswith(prefix):
                return line.split(":", 1)[1].strip()
    return None


def parse_request_body(body):
    """
    Parse an incoming HTTP request body as JSON (or URL-encoded form fallback).

    :param body (str | bytes): Raw HTTP request body.
    :rtype: dict | None
    """
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")

    if not body or not body.strip():
        return None

    # Primary contract: application/json
    try:
        parsed = json.loads(body)
        if isinstance(parsed, dict):
            return parsed
        return None
    except json.JSONDecodeError:
        pass

    # Fallback: application/x-www-form-urlencoded (e.g., from www/login.html)
    if "=" in body:
        form_data = parse_qs(body, keep_blank_values=False)
        if form_data:
            return {k: v[0] for k, v in form_data.items()}

    return None


def extract_cookies(headers):
    """
    Extract cookie key-value pairs from HTTP request headers (RFC 6265).

    Supports both dict-based headers (case-insensitive) and raw header strings.

    :param headers (dict | str): Incoming request headers.
    :rtype: dict
    """
    cookie_header = ""

    if isinstance(headers, dict):
        for key, val in headers.items():
            if key.lower() == "cookie":
                cookie_header = val
                break
    elif isinstance(headers, str):
        for line in headers.splitlines():
            if line.lower().startswith("cookie:"):
                cookie_header = line.split(":", 1)[1].strip()
                break

    cookies = {}
    if cookie_header:
        for pair in cookie_header.split(";"):
            if "=" in pair:
                k, v = pair.strip().split("=", 1)
                cookies[k.strip()] = v.strip()
    return cookies


def authenticate_session(headers):
    """
    Validate the client's session_id cookie for stateless HTTP authentication.

    :param headers (dict | str): Incoming request headers.
    :rtype: tuple (bool, str | None) -> (is_authenticated, username)
    """
    cookies = extract_cookies(headers)
    session_id = cookies.get("session_id")
    if not session_id:
        return False, None

    # TODO (Integration - Chau): Replace or delegate to Chau's session validator
    with _STATE_LOCK:
        username = _ACTIVE_SESSIONS.get(session_id)

    if username is None:
        return False, None
    return True, username


# ---------------------------------------------------------------------------
# Teammate Module Integration Adapters (Chau, Nhan, Nam)
# ---------------------------------------------------------------------------

def _auth_verify_user(username, password):
    """
    Integration hook for Chau's Authentication module.

    Validates credentials and returns a newly issued session_id on success,
    or None if authentication fails.
    """
    # TODO (Integration - Chau): Import and call Chau's auth module here, e.g.:
    #   from apps.auth import verify_credentials
    #   return verify_credentials(username, password)
    if not username or not password:
        return None

    # Default standalone fallback for testing
    session_id = uuid.uuid4().hex
    with _STATE_LOCK:
        _ACTIVE_SESSIONS[session_id] = username
    return session_id


def _tracker_register_peer(peer_ip, peer_port, username="anonymous"):
    """
    Integration hook for Nhan's Tracker & Discovery module (register peer).
    """
    # TODO (Integration - Nhan): Import and call Nhan's tracker module here, e.g.:
    #   from apps.tracker import register_peer
    #   register_peer(peer_ip, peer_port, username)
    with _STATE_LOCK:
        _PEER_REGISTRY[(peer_ip, peer_port)] = {
            "ip": peer_ip,
            "port": peer_port,
            "username": username,
        }


def _tracker_get_online_peers():
    """
    Integration hook for Nhan's Tracker & Discovery module (list online peers).
    """
    # TODO (Integration - Nhan): Import and call Nhan's discovery module here, e.g.:
    #   from apps.tracker import get_online_peers
    #   return get_online_peers()
    with _STATE_LOCK:
        return list(_PEER_REGISTRY.values())


def _p2p_send_bytes(dest_ip, dest_port, content):
    """
    Integration hook for Nam's P2P Dispatcher module (direct TCP byte stream).

    Establishes a direct connection to the target peer (P2P Phase of Chapter 2)
    and transmits the payload without routing data through the central server.
    """
    # TODO (Integration - Nam): Import and call Nam's P2P sender interface here, e.g.:
    #   from apps.p2p import send_peer_message
    #   return send_peer_message(dest_ip, dest_port, content)
    payload_bytes = content.encode("utf-8") if isinstance(content, str) else content
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as peer_sock:
        peer_sock.settimeout(5.0)
        peer_sock.connect((dest_ip, int(dest_port)))
        peer_sock.sendall(payload_bytes)


def _channel_list(username):
    """
    Integration hook for Trong Nhan's Channel Management module (list channels).

    Returns all available channels along with membership status for `username`.
    """
    # TODO (Integration - Trong Nhan): Import and call Trong Nhan's channel list API, e.g.:
    #   from apps.channel import list_channels
    #   return list_channels(username)
    with _STATE_LOCK:
        return [
            {
                "name": ch["name"],
                "creator": ch["creator"],
                "member_count": len(ch["members"]),
                "joined": username in ch["members"],
            }
            for ch in _CHANNELS.values()
        ]


def _channel_create(channel_name, creator):
    """
    Integration hook for Trong Nhan's Channel Management module (create channel).

    Returns (created: bool, channel_info: dict).
    """
    # TODO (Integration - Trong Nhan): Import and call Trong Nhan's channel create API, e.g.:
    #   from apps.channel import create_channel
    #   return create_channel(channel_name, creator)
    with _STATE_LOCK:
        if channel_name in _CHANNELS:
            return False, None
        _CHANNELS[channel_name] = {
            "name": channel_name,
            "creator": creator,
            "members": {creator},
        }
        _CHANNEL_MESSAGES[channel_name] = []
        return True, {
            "name": channel_name,
            "creator": creator,
            "member_count": 1,
        }


def _channel_join(channel_name, username):
    """
    Integration hook for Trong Nhan's Channel Management module (join channel).

    Returns True if joined successfully, or False if the channel does not exist.
    """
    # TODO (Integration - Trong Nhan): Import and call Trong Nhan's channel join API, e.g.:
    #   from apps.channel import join_channel
    #   return join_channel(channel_name, username)
    with _STATE_LOCK:
        ch = _CHANNELS.get(channel_name)
        if ch is None:
            return False
        ch["members"].add(username)
        return True


def _channel_get_messages(channel_name):
    """
    Integration hook for Huy's Message Synchronization module (read channel history).

    Messages are append-only and immutable (cannot be edited or deleted).
    Returns None if the channel does not exist.
    """
    # TODO (Integration - Huy): Import and call Huy's message sync API, e.g.:
    #   from apps.messaging import get_channel_messages
    #   return get_channel_messages(channel_name)
    with _STATE_LOCK:
        if channel_name not in _CHANNEL_MESSAGES:
            return None
        return list(_CHANNEL_MESSAGES[channel_name])


def _channel_send_message(channel_name, sender, content):
    """
    Integration hook for Huy's P2P Broadcast & Message Protocol module.

    Appends an immutable message record to the channel and broadcasts it to
    online channel peers. Returns (ok: bool, msg_record: dict | None).
    """
    # TODO (Integration - Huy): Import and call Huy's broadcast protocol API, e.g.:
    #   from apps.messaging import broadcast_channel_message
    #   return broadcast_channel_message(channel_name, sender, content)
    with _STATE_LOCK:
        if channel_name not in _CHANNELS:
            return False, None
        msg_record = {
            "id": uuid.uuid4().hex,
            "channel": channel_name,
            "sender": sender,
            "content": content,
            "timestamp": int(time.time()),
            "immutable": True,
        }
        _CHANNEL_MESSAGES[channel_name].append(msg_record)
        return True, dict(msg_record)


def _ui_load_html(filename):
    """
    Integration hook for Huy's Chat UI static HTML assets in `www/`.

    :param filename (str): HTML filename inside the `www/` directory.
    :rtype: str | None
    """
    # TODO (Integration - Huy): Point to Huy's finalized Chat UI template if renamed
    filepath = os.path.join(WWW_DIR, filename)
    if not os.path.isfile(filepath):
        return None
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# RESTful Route Definitions (Uniform Interface & Resource Identification)
# ---------------------------------------------------------------------------

@app.route("/login", methods=["POST"])
def login(headers="", body=""):
    """
    Authenticate a user and issue an HTTP session cookie (RFC 6265).

    Contract:
      Request  : POST /login with JSON {"username": "...", "password": "..."}
      Response : 200 OK + Set-Cookie: session_id=... on success
                 400 Bad Request on malformed payload
                 401 Unauthorized on invalid credentials
    """
    data = parse_request_body(body)
    if data is None or "username" not in data or "password" not in data:
        return build_json_response(
            400,
            "Bad Request",
            {"error": "Missing or invalid JSON body. Required fields: username, password."},
        )

    username = str(data.get("username", "")).strip()
    password = str(data.get("password", ""))

    session_id = _auth_verify_user(username, password)
    if not session_id:
        return build_json_response(
            401,
            "Unauthorized",
            {"error": "Invalid username or password."},
        )

    print(f"[SampleApp] Authenticated user '{username}', issued session_id={session_id}")
    return build_json_response(
        200,
        "OK",
        {"message": "Login successful", "username": username, "session_id": session_id},
        extra_headers={"Set-Cookie": f"session_id={session_id}; HttpOnly; Path=/"},
    )


@app.route("/submit-info", methods=["POST"])
def submit_info(headers="", body=""):
    """
    Register an online peer's network address with the central Tracker.

    Implements the centralized Client-Server registration step of the Hybrid
    Architecture (Chapter 2). Requires a valid session_id cookie.

    Contract:
      Request  : POST /submit-info with JSON {"ip": "...", "port": 9001}
      Response : 200 OK on success, 400 Bad Request, or 401 Unauthorized
    """
    is_auth, username = authenticate_session(headers)
    if not is_auth or username is None:
        return build_json_response(
            401,
            "Unauthorized",
            {"error": "Authentication required. Please login first."},
        )

    username = str(username)
    data = parse_request_body(body)
    if data is None:
        return build_json_response(
            400,
            "Bad Request",
            {"error": "Invalid JSON body."},
        )

    peer_ip = data.get("ip") or data.get("peer_ip")
    peer_port = data.get("port") if "port" in data else data.get("peer_port")

    if not peer_ip or peer_port is None:
        return build_json_response(
            400,
            "Bad Request",
            {"error": "Missing required fields: ip, port."},
        )

    try:
        peer_port = int(peer_port)
        if not (1 <= peer_port <= 65535):
            raise ValueError("Port out of range")
    except (TypeError, ValueError):
        return build_json_response(
            400,
            "Bad Request",
            {"error": "Field 'port' must be a valid integer between 1 and 65535."},
        )

    _tracker_register_peer(str(peer_ip), peer_port, username=username)
    print(f"[SampleApp] Registered peer {peer_ip}:{peer_port} for user '{username}'")

    return build_json_response(
        200,
        "OK",
        {
            "message": "Peer info submitted successfully",
            "peer": {"ip": str(peer_ip), "port": peer_port, "username": username},
        },
    )


@app.route("/get-list", methods=["GET"])
def get_list(headers="", body=""):
    """
    Retrieve the list of currently online peers from the Tracker.

    GET is a safe, idempotent REST method used solely to query resource state.
    Requires a valid session_id cookie.

    Contract:
      Request  : GET /get-list
      Response : 200 OK with JSON {"peers": [...], "count": N}
                 401 Unauthorized if session cookie is missing/invalid
    """
    is_auth, _ = authenticate_session(headers)
    if not is_auth:
        return build_json_response(
            401,
            "Unauthorized",
            {"error": "Authentication required. Please login first."},
        )

    peers = _tracker_get_online_peers()
    return build_json_response(
        200,
        "OK",
        {"peers": peers, "count": len(peers)},
    )


@app.route("/send-peer", methods=["POST"])
def send_peer(headers="", body=""):
    """
    Dispatch a message directly to a target peer over a P2P TCP byte stream.

    Implements the Peer-to-Peer (P2P) communication phase after peer discovery
    and acts as a stateless messaging dispatcher. Requires a valid session_id cookie.

    Contract:
      Request  : POST /send-peer with JSON {"dest_ip": "...", "dest_port": 9001, "content": "..."}
      Response : 200 OK on delivery, 400 Bad Request, 401 Unauthorized, or 502 Bad Gateway
    """
    is_auth, username = authenticate_session(headers)
    if not is_auth:
        return build_json_response(
            401,
            "Unauthorized",
            {"error": "Authentication required. Please login first."},
        )

    data = parse_request_body(body)
    if data is None:
        return build_json_response(
            400,
            "Bad Request",
            {"error": "Invalid JSON body."},
        )

    dest_ip = data.get("dest_ip")
    dest_port = data.get("dest_port")
    content = data.get("content")

    if not dest_ip or dest_port is None or content is None:
        return build_json_response(
            400,
            "Bad Request",
            {"error": "Missing required fields: dest_ip, dest_port, content."},
        )

    try:
        dest_port = int(dest_port)
        if not (1 <= dest_port <= 65535):
            raise ValueError("Port out of range")
    except (TypeError, ValueError):
        return build_json_response(
            400,
            "Bad Request",
            {"error": "Field 'dest_port' must be a valid integer between 1 and 65535."},
        )

    try:
        _p2p_send_bytes(str(dest_ip), dest_port, str(content))
    except OSError as exc:
        return build_json_response(
            502,
            "Bad Gateway",
            {"error": f"Failed to deliver message to {dest_ip}:{dest_port}: {exc}"},
        )

    print(f"[SampleApp] User '{username}' dispatched P2P message to {dest_ip}:{dest_port}")
    return build_json_response(
        200,
        "OK",
        {
            "message": "P2P message sent",
            "dest_ip": str(dest_ip),
            "dest_port": dest_port,
        },
    )


@app.route("/channels", methods=["GET"])
def list_channels(headers="", body=""):
    """
    Retrieve the list of available chat channels and user membership status.

    Contract:
      Request  : GET /channels (requires Cookie: session_id=...)
      Response : 200 OK with JSON {"channels": [...], "count": N}
                 401 Unauthorized if session cookie is missing/invalid
    """
    is_auth, username = authenticate_session(headers)
    if not is_auth:
        return build_json_response(
            401,
            "Unauthorized",
            {"error": "Authentication required. Please login first."},
        )

    channels = _channel_list(username)
    return build_json_response(
        200,
        "OK",
        {"channels": channels, "count": len(channels)},
    )


@app.route("/channels/create", methods=["POST"])
def create_channel(headers="", body=""):
    """
    Create a new chat channel and automatically join the creator.

    Contract:
      Request  : POST /channels/create with JSON {"channel_name": "..."}
      Response : 200 OK on creation, 400 Bad Request if missing/duplicate, 401 Unauthorized
    """
    is_auth, username = authenticate_session(headers)
    if not is_auth:
        return build_json_response(
            401,
            "Unauthorized",
            {"error": "Authentication required. Please login first."},
        )

    data = parse_request_body(body)
    if data is None or not str(data.get("channel_name", "")).strip():
        return build_json_response(
            400,
            "Bad Request",
            {"error": "Missing required field: channel_name."},
        )

    channel_name = str(data["channel_name"]).strip()
    created, info = _channel_create(channel_name, username)
    if not created:
        return build_json_response(
            400,
            "Bad Request",
            {"error": f"Channel '{channel_name}' already exists."},
        )

    print(f"[SampleApp] User '{username}' created channel '{channel_name}'")
    return build_json_response(
        200,
        "OK",
        {"message": "Channel created", "channel": info},
    )


@app.route("/channels/join", methods=["POST"])
def join_channel(headers="", body=""):
    """
    Join an existing chat channel.

    Contract:
      Request  : POST /channels/join with JSON {"channel_name": "..."}
      Response : 200 OK on success, 400 Bad Request, 401 Unauthorized, or 404 Not Found
    """
    is_auth, username = authenticate_session(headers)
    if not is_auth:
        return build_json_response(
            401,
            "Unauthorized",
            {"error": "Authentication required. Please login first."},
        )

    data = parse_request_body(body)
    if data is None or not str(data.get("channel_name", "")).strip():
        return build_json_response(
            400,
            "Bad Request",
            {"error": "Missing required field: channel_name."},
        )

    channel_name = str(data["channel_name"]).strip()
    if not _channel_join(channel_name, username):
        return build_json_response(
            404,
            "Not Found",
            {"error": f"Channel '{channel_name}' does not exist."},
        )

    print(f"[SampleApp] User '{username}' joined channel '{channel_name}'")
    return build_json_response(
        200,
        "OK",
        {"message": f"Joined channel '{channel_name}'", "channel_name": channel_name},
    )


@app.route("/channels/messages", methods=["GET"])
def get_channel_messages(headers="", body=""):
    """
    Retrieve immutable, append-only message history for a channel.

    Note: Per application requirements, messages cannot be edited or deleted
    (no PUT/PATCH/DELETE endpoints are exposed).

    Contract:
      Request  : GET /channels/messages with header `X-Channel-Name: <name>`
                 (or JSON body {"channel_name": "<name>"})
      Response : 200 OK with JSON {"channel": "...", "messages": [...], "count": N}
                 400 Bad Request, 401 Unauthorized, or 404 Not Found
    """
    is_auth, _ = authenticate_session(headers)
    if not is_auth:
        return build_json_response(
            401,
            "Unauthorized",
            {"error": "Authentication required. Please login first."},
        )

    channel_name = extract_header_value(headers, "X-Channel-Name")
    if not channel_name:
        data = parse_request_body(body)
        if data:
            channel_name = data.get("channel_name")

    if not channel_name or not str(channel_name).strip():
        return build_json_response(
            400,
            "Bad Request",
            {"error": "Missing channel_name (pass via X-Channel-Name header or JSON body)."},
        )

    channel_name = str(channel_name).strip()
    messages = _channel_get_messages(channel_name)
    if messages is None:
        return build_json_response(
            404,
            "Not Found",
            {"error": f"Channel '{channel_name}' does not exist."},
        )

    return build_json_response(
        200,
        "OK",
        {"channel": channel_name, "messages": messages, "count": len(messages)},
    )


@app.route("/channels/send", methods=["POST"])
def send_channel_message(headers="", body=""):
    """
    Send (broadcast) an immutable message to a chat channel.

    Contract:
      Request  : POST /channels/send with JSON {"channel_name": "...", "content": "..."}
      Response : 200 OK with created message record, 400 Bad Request, 401 Unauthorized, or 404 Not Found
    """
    is_auth, username = authenticate_session(headers)
    if not is_auth:
        return build_json_response(
            401,
            "Unauthorized",
            {"error": "Authentication required. Please login first."},
        )

    data = parse_request_body(body)
    if data is None:
        return build_json_response(
            400,
            "Bad Request",
            {"error": "Invalid JSON body."},
        )

    channel_name = str(data.get("channel_name", "")).strip()
    content = data.get("content")
    if not channel_name or content is None or not str(content).strip():
        return build_json_response(
            400,
            "Bad Request",
            {"error": "Missing required fields: channel_name, content."},
        )

    ok, msg_record = _channel_send_message(channel_name, username, str(content))
    if not ok:
        return build_json_response(
            404,
            "Not Found",
            {"error": f"Channel '{channel_name}' does not exist."},
        )

    print(f"[SampleApp] User '{username}' sent message to channel '{channel_name}'")
    return build_json_response(
        200,
        "OK",
        {"message": "Message sent to channel", "data": msg_record},
    )


@app.route("/chat", methods=["GET"])
def chat_ui(headers="", body=""):
    """
    Serve the Hybrid P2P Chat Web UI page (`www/chat.html`).

    Contract:
      Request  : GET /chat
      Response : 200 OK with Content-Type: text/html, or 404 Not Found
    """
    html = _ui_load_html("chat.html")
    if html is None:
        return build_html_response(404, "Not Found", "<h1>404 Not Found - chat.html missing</h1>")
    return build_html_response(200, "OK", html)


@app.route("/echo", methods=["POST"])
def echo(headers="", body=""):
    """
    Health-check / echo endpoint for testing JSON request-response handling.
    """
    data = parse_request_body(body)
    if data is None:
        return build_json_response(400, "Bad Request", {"error": "Invalid JSON"})
    return build_json_response(200, "OK", {"received": data})


def create_sampleapp(ip, port):
    """
    Bind the StreapRous WebApp to the specified IP and port and start listening.

    Implements the always-on server host initialization (Chapter 2 Socket API).

    :param ip (str): IP address to bind the server socket.
    :param port (int): TCP port number to listen on.
    """
    app.prepare_address(ip, port)
    app.run()
