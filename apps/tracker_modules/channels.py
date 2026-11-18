"""Quan ly channel va danh sach thanh vien."""

from typing import List, Optional
from uuid import uuid4
from .models import Channel, TrackerError


class ChannelMixin:
    def create_channel(self, owner_id: object, name: object, now: Optional[float] = None) -> dict:
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

    def list_channels_for_peer(self, peer_id: object) -> List[dict]:
        """Return every channel joined by a peer, including inactive peers."""
        with self._lock:
            peer = self._require_peer_locked(peer_id, active=False)
            channels = [
                channel.as_dict()
                for channel in self._channels.values()
                if peer.peer_id in channel.member_ids
            ]
        return sorted(channels, key=lambda channel: (channel["name"], channel["channel_id"]))

    def list_channel_members(self, channel_id: object, active_only: bool = True) -> List[dict]:
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
