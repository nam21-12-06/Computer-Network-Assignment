"""Dang ky peer va theo doi trang thai hoat dong."""

import ipaddress
from typing import List, Optional
from .models import Channel, Peer, TrackerError


class PeerRegistryMixin:
    @staticmethod
    def _required_text(value: object, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise TrackerError(f"{field_name} is required")
        return value.strip()

    @staticmethod
    def _valid_ip(value: object) -> str:
        ip = PeerRegistryMixin._required_text(value, "ip")
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

    def _now(self, now: Optional[float]) -> float:
        return self._clock() if now is None else now

    def _cleanup_locked(self, now: float) -> List[str]:
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
        now: Optional[float] = None,
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
        ip: Optional[object] = None,
        listen_port: Optional[object] = None,
        now: Optional[float] = None,
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

    def cleanup_expired_peers(self, now: Optional[float] = None) -> List[str]:
        """Mark peers that missed the heartbeat deadline as inactive."""
        with self._lock:
            return self._cleanup_locked(self._now(now))

    def list_active_peers(
        self,
        exclude_peer_id: Optional[object] = None,
        now: Optional[float] = None,
    ) -> List[dict]:
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

    def disconnect_peer(self, peer_id: object, now: Optional[float] = None) -> dict:
        """Immediately remove a peer from the discovery list."""
        with self._lock:
            peer = self._require_peer_locked(peer_id, active=False)
            peer.status = "inactive"
            peer.last_seen = self._now(now)
            return peer.as_dict()

    def connect_peer(self, requester_peer_id: object, target_peer_id: object, now: Optional[float] = None) -> dict:
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
