"""Tracker ket hop chuc nang quan ly peer va channel."""

import threading
import time
from typing import Callable
from .tracker_modules.models import Peer, Channel, TrackerError
from .tracker_modules.registry import PeerRegistryMixin
from .tracker_modules.channels import ChannelMixin

DEFAULT_PEER_TTL_SECONDS = 60


class TrackerService(PeerRegistryMixin, ChannelMixin):
    """Quan ly danh sach peer va channel trong bo nho."""

    def __init__(
        self,
        peer_ttl_seconds: int = DEFAULT_PEER_TTL_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if peer_ttl_seconds <= 0:
            raise ValueError("peer_ttl_seconds must be positive")
        self._peer_ttl_seconds = peer_ttl_seconds
        self._clock = clock
        self._peers = {}
        self._channels = {}
        self._lock = threading.RLock()
