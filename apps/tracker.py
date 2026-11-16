"""In-memory tracker and channel services for the hybrid P2P chat app.

The tracker stores connection metadata only.  It never routes chat messages:
after discovery, peers establish their own direct transport connection.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import ipaddress
import threading
import time
from typing import Callable
from uuid import uuid4


DEFAULT_PEER_TTL_SECONDS = 60


class TrackerError(ValueError):
    """A validation or state error that can be returned by the HTTP API."""

    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass
class Peer:
    """A peer endpoint currently known by the tracker."""

    peer_id: str
    user_id: str
    ip: str
    listen_port: int
    registered_at: float
    last_seen: float
    status: str = "active"

    def as_dict(self) -> dict:
        return {
            "peer_id": self.peer_id,
            "user_id": self.user_id,
            "ip": self.ip,
            "listen_port": self.listen_port,
            "status": self.status,
            "registered_at": self.registered_at,
            "last_seen": self.last_seen,
        }


@dataclass
class Channel:
    """A channel and the peer IDs that currently belong to it."""

    channel_id: str
    name: str
    owner_id: str
    created_at: float
    member_ids: set[str] = field(default_factory=set)

    def as_dict(self) -> dict:
        return {
            "channel_id": self.channel_id,
            "name": self.name,
            "owner_id": self.owner_id,
            "created_at": self.created_at,
            "member_count": len(self.member_ids),
        }


class TrackerService:
    """Thread-safe active-peer registry and channel membership service."""

    def __init__(
        self,
        peer_ttl_seconds: int = DEFAULT_PEER_TTL_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if peer_ttl_seconds <= 0:
            raise ValueError("peer_ttl_seconds must be positive")
        self._peer_ttl_seconds = peer_ttl_seconds
        self._clock = clock
        self._peers: dict[str, Peer] = {}
        self._channels: dict[str, Channel] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _required_text(value: object, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise TrackerError(f"{field_name} is required")
        return value.strip()

    @staticmethod
    def _valid_ip(value: object) -> str:
        ip = TrackerService._required_text(value, "ip")
        try:
            ipaddress.ip_address(ip)
        except ValueError as error:
            raise TrackerError("ip must be a valid IP address") from error
        return ip

    @staticmethod
    def _valid_port(value: object) -> int:
        if isinstance(value, bool):
            raise TrackerError("listen_port must be an integer from 1 to 65535")
        try:
            port = int(value)
        except (TypeError, ValueError) as error:
            raise TrackerError("listen_port must be an integer from 1 to 65535") from error
        if not 1 <= port <= 65535:
            raise TrackerError("listen_port must be an integer from 1 to 65535")
        return port

    def _now(self, now: float | None) -> float:
        return self._clock() if now is None else now

    def _cleanup_locked(self, now: float) -> list[str]:
        expired_peer_ids = []
        for peer in self._peers.values():
            if peer.status == "active" and now - peer.last_seen > self._peer_ttl_seconds:
                peer.status = "inactive"
                expired_peer_ids.append(peer.peer_id)
        return expired_peer_ids

    def _require_peer_locked(self, peer_id: object, active: bool = True) -> Peer:
        normalized_peer_id = self._required_text(peer_id, "peer_id")
        peer = self._peers.get(normalized_peer_id)
        if peer is None:
            raise TrackerError("peer was not registered", 404)
        if active and peer.status != "active":
            raise TrackerError("peer is not active", 409)
        return peer

    def _require_channel_locked(self, channel_id: object) -> Channel:
        normalized_channel_id = self._required_text(channel_id, "channel_id")
        channel = self._channels.get(normalized_channel_id)
        if channel is None:
            raise TrackerError("channel was not found", 404)
        return channel

    def register_peer(
        self,
        peer_id: object,
        user_id: object,
        ip: object,
        listen_port: object,
        now: float | None = None,
    ) -> dict:
        """Create or refresh a peer's publicly discoverable endpoint."""
        normalized_peer_id = self._required_text(peer_id, "peer_id")
        normalized_user_id = self._required_text(user_id, "user_id")
        normalized_ip = self._valid_ip(ip)
        normalized_port = self._valid_port(listen_port)
        current_time = self._now(now)

        with self._lock:
            existing = self._peers.get(normalized_peer_id)
            registered_at = existing.registered_at if existing else current_time
            peer = Peer(
                peer_id=normalized_peer_id,
                user_id=normalized_user_id,
                ip=normalized_ip,
                listen_port=normalized_port,
                registered_at=registered_at,
                last_seen=current_time,
            )
            self._peers[normalized_peer_id] = peer
            return peer.as_dict()

    def heartbeat(
        self,
        peer_id: object,
        ip: object | None = None,
        listen_port: object | None = None,
        now: float | None = None,
    ) -> dict:
        """Refresh liveness and optionally refresh a peer endpoint."""
        current_time = self._now(now)
        with self._lock:
            peer = self._require_peer_locked(peer_id, active=False)
            if ip is not None:
                peer.ip = self._valid_ip(ip)
            if listen_port is not None:
                peer.listen_port = self._valid_port(listen_port)
            peer.last_seen = current_time
            peer.status = "active"
            return peer.as_dict()

    def cleanup_expired_peers(self, now: float | None = None) -> list[str]:
        """Mark peers that missed the heartbeat deadline as inactive."""
        with self._lock:
            return self._cleanup_locked(self._now(now))

    def list_active_peers(
        self,
        exclude_peer_id: object | None = None,
        now: float | None = None,
    ) -> list[dict]:
        """Return active peers, optionally excluding the requesting peer."""
        excluded = str(exclude_peer_id).strip() if exclude_peer_id is not None else None
        with self._lock:
            self._cleanup_locked(self._now(now))
            peers = [
                peer.as_dict()
                for peer in self._peers.values()
                if peer.status == "active" and peer.peer_id != excluded
            ]
        return sorted(peers, key=lambda peer: peer["peer_id"])

    def disconnect_peer(self, peer_id: object, now: float | None = None) -> dict:
        """Immediately remove a peer from the discovery list."""
        with self._lock:
            peer = self._require_peer_locked(peer_id, active=False)
            peer.status = "inactive"
            peer.last_seen = self._now(now)
            return peer.as_dict()

    def connect_peer(self, requester_peer_id: object, target_peer_id: object, now: float | None = None) -> dict:
        """Return an active target's endpoint for direct P2P connection setup."""
        with self._lock:
            self._cleanup_locked(self._now(now))
            requester = self._require_peer_locked(requester_peer_id)
            target = self._require_peer_locked(target_peer_id)
            if requester.peer_id == target.peer_id:
                raise TrackerError("a peer cannot connect to itself")
            return {
                "peer_id": target.peer_id,
                "user_id": target.user_id,
                "ip": target.ip,
                "listen_port": target.listen_port,
            }

    def create_channel(self, owner_id: object, name: object, now: float | None = None) -> dict:
        """Create a channel and add the active owner as its first member."""
        normalized_name = self._required_text(name, "name")
        current_time = self._now(now)
        with self._lock:
            owner = self._require_peer_locked(owner_id)
            channel = Channel(
                channel_id=uuid4().hex,
                name=normalized_name,
                owner_id=owner.peer_id,
                created_at=current_time,
                member_ids={owner.peer_id},
            )
            self._channels[channel.channel_id] = channel
            return channel.as_dict()

    def join_channel(self, peer_id: object, channel_id: object) -> dict:
        """Add an active peer to a channel. Joining twice is idempotent."""
        with self._lock:
            peer = self._require_peer_locked(peer_id)
            channel = self._require_channel_locked(channel_id)
            channel.member_ids.add(peer.peer_id)
            return channel.as_dict()

    def leave_channel(self, peer_id: object, channel_id: object) -> dict:
        """Remove a peer and transfer ownership or delete an empty channel."""
        with self._lock:
            peer = self._require_peer_locked(peer_id, active=False)
            channel = self._require_channel_locked(channel_id)
            if peer.peer_id not in channel.member_ids:
                raise TrackerError("peer is not a member of this channel", 409)

            channel.member_ids.remove(peer.peer_id)
            if not channel.member_ids:
                del self._channels[channel.channel_id]
                return {"channel_id": channel.channel_id, "deleted": True}

            if channel.owner_id == peer.peer_id:
                channel.owner_id = sorted(channel.member_ids)[0]
            return channel.as_dict()

    def list_channels_for_peer(self, peer_id: object) -> list[dict]:
        """Return every channel joined by a peer, including inactive peers."""
        with self._lock:
            peer = self._require_peer_locked(peer_id, active=False)
            channels = [
                channel.as_dict()
                for channel in self._channels.values()
                if peer.peer_id in channel.member_ids
            ]
        return sorted(channels, key=lambda channel: (channel["name"], channel["channel_id"]))

    def list_channel_members(self, channel_id: object, active_only: bool = True) -> list[dict]:
        """Return channel members, normally limited to live P2P targets."""
        with self._lock:
            # Cap nhat trang thai truoc khi loc, ke ca khi chua ai goi discovery.
            self._cleanup_locked(self._now(None))
            channel = self._require_channel_locked(channel_id)
            members = []
            for peer_id in channel.member_ids:
                peer = self._peers.get(peer_id)
                if peer is not None and (not active_only or peer.status == "active"):
                    members.append(peer.as_dict())
        return sorted(members, key=lambda peer: peer["peer_id"])
