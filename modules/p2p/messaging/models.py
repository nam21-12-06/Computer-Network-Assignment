"""
Define valid message structure:
    Version       : 1 byte
    Type          : 1 byte, 0x10=Direct, 0x11=Group
    Message_ID    : 16 bytes
    Sender_ID     : 4 bytes
    Target_ID     : 4 bytes
    Sender_Seq    : 8 bytes
    Timestamp_MS  : 8 bytes
    Text_Length   : 2 bytes
    Text          : UTF-8(N)  (1-4096 bytes)
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Union
from uuid import UUID, RFC_4122


VERSION = 1
DIRECT = 0x10
GROUP = 0x11
SYNC_REQUEST = 0x12
SYNC_END = 0x13
CHAT_KIND = (DIRECT, GROUP)
MAX_TEXT_BYTES = 4096
MAX_SYNC_BATCH = 100
MAX_U32 = (1 << 32) - 1
MAX_SEQUENCE = (1 << 63) - 1
MAX_TIMESTAMP_MS = (1 << 63) - 1


class ProtocolError(ValueError):
    pass


def require_int(label: str, value: int, lo: int, hi: int) -> None:
    if (type(value) is not int) or (not lo <= value <= hi):
        raise ProtocolError(f"{label} must be between {lo} and {hi}")


def check_scope(kind: int, target_id: int) -> None:
    """Validate message type and target id"""
    if (type(kind) is not int) or (kind not in CHAT_KIND):
        raise ProtocolError("Invalid message kind")

    min_target = 0
    if kind == DIRECT:
        min_target = 1    
    require_int("target_id", target_id, min_target, MAX_U32)
    
    
@dataclass(frozen = True, slots = True)    
class ChatMessage:
    kind: int
    msg_id: UUID
    sender_id: int
    target_id: int
    sender_seq: int
    timestamp_ms: int 
    text: str
    
    def __post_init__(self) -> None:
        check_scope(self.kind, self.target_id)
        if (
            not isinstance(self.msg_id, UUID)
            or self.msg_id.version != 4
            or self.msg_id.variant != RFC_4122
        ):
            raise ProtocolError("msg_id must be UUIDv4")
        
        require_int("sender_id", self.sender_id, 1, MAX_U32)
        require_int("sender_seq", self.sender_seq, 1, MAX_SEQUENCE)
        require_int("timestamp_ms", self.timestamp_ms, 1, MAX_TIMESTAMP_MS)
        
        if not isinstance(self.text, str):
            raise ProtocolError("text must be string")
        
        encoded = self.text.encode("utf-8", errors="strict")
        
        if not 1 <= len(encoded) <= MAX_TEXT_BYTES:
            raise ProtocolError("Invalid text length")
        
        
@dataclass(frozen=True, slots=True)
class SyncRequest:
    scope: int
    target_id: int
    after_seq: int
    limit: int = MAX_SYNC_BATCH

    def __post_init__(self) -> None:
        check_scope(self.scope, self.target_id)
        require_int("after_seq", self.after_seq, 0, MAX_SEQUENCE)
        require_int("limit", self.limit, 1, MAX_SYNC_BATCH)


@dataclass(frozen=True, slots=True)
class SyncEnd:
    scope: int
    target_id: int
    last_seq: int
    has_more: bool

    def __post_init__(self) -> None:
        check_scope(self.scope, self.target_id)
        require_int("last_seq", self.last_seq, 0, MAX_SEQUENCE)

        if type(self.has_more) is not bool:
            raise ProtocolError("has_more must be bool")


Message = Union[ChatMessage, SyncRequest, SyncEnd]