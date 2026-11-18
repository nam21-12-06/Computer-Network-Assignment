"""Du lieu peer, channel va loi tracker."""

class TrackerError(ValueError):
    """A validation or state error that can be returned by the HTTP API."""

    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


class Peer:
    """A peer endpoint currently known by the tracker."""

    def __init__(self, peer_id, user_id, ip, listen_port, registered_at,
                 last_seen, status="active"):
        self.peer_id = peer_id
        self.user_id = user_id
        self.ip = ip
        self.listen_port = listen_port
        self.registered_at = registered_at
        self.last_seen = last_seen
        self.status = status

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


class Channel:
    """A channel and the peer IDs that currently belong to it."""

    def __init__(self, channel_id, name, owner_id, created_at, member_ids=None):
        self.channel_id = channel_id
        self.name = name
        self.owner_id = owner_id
        self.created_at = created_at
        self.member_ids = set() if member_ids is None else set(member_ids)

    def as_dict(self) -> dict:
        return {
            "channel_id": self.channel_id,
            "name": self.name,
            "owner_id": self.owner_id,
            "created_at": self.created_at,
            "member_count": len(self.member_ids),
        }
