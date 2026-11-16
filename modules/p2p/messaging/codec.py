import struct
from uuid import UUID

from .models import (
    VERSION, DIRECT, GROUP,
    SYNC_REQUEST, SYNC_END,
    MAX_TEXT_BYTES,
    ChatMessage, SyncRequest, SyncEnd,
    Message, ProtocolError,
)

_CHAT = struct.Struct("!BB16sIIQQH")
_REQUEST = struct.Struct("!BBBIQH")
_END = struct.Struct("!BBBIQB")

MAX_BODY_BYTES = _CHAT.size + MAX_TEXT_BYTES


def encode_body(message: Message) -> bytes:
    if isinstance(message, ChatMessage):
        raw = message.text.encode("utf-8", errors="strict")
        return _CHAT.pack(
            VERSION,
            message.kind,
            message.msg_id.bytes,
            message.sender_id,
            message.target_id,
            message.sender_seq,
            message.timestamp_ms,
            len(raw),
        ) + raw

    if isinstance(message, SyncRequest):
        return _REQUEST.pack(
            VERSION,
            SYNC_REQUEST,
            message.scope,
            message.target_id,
            message.after_seq,
            message.limit,
        )

    if isinstance(message, SyncEnd):
        return _END.pack(
            VERSION,
            SYNC_END,
            message.scope,
            message.target_id,
            message.last_seq,
            int(message.has_more),
        )

    raise ProtocolError("unknown message model")


def decode_body(raw: bytes) -> Message:
    if type(raw) is not bytes:
        raise ProtocolError("body must be bytes")

    if not 2 <= len(raw) <= MAX_BODY_BYTES:
        raise ProtocolError("invalid body length")

    version, kind = raw[0], raw[1]

    if version != VERSION:
        raise ProtocolError("unsupported version")

    if kind in (DIRECT, GROUP):
        if len(raw) < _CHAT.size:
            raise ProtocolError("chat body too short")

        (
            _, _, uuid_bytes, sender, target,
            seq, stamp, text_len
        ) = _CHAT.unpack_from(raw)

        if not 1 <= text_len <= MAX_TEXT_BYTES:
            raise ProtocolError("invalid text length")

        if len(raw) != _CHAT.size + text_len:
            raise ProtocolError("chat text length mismatch")

        text = raw[_CHAT.size:].decode("utf-8", errors="strict")

        return ChatMessage(
            kind=kind,
            msg_id=UUID(bytes=uuid_bytes),
            sender_id=sender,
            target_id=target,
            sender_seq=seq,
            timestamp_ms=stamp,
            text=text,
        )

    if kind == SYNC_REQUEST:
        if len(raw) != _REQUEST.size:
            raise ProtocolError("invalid SyncRequest length")

        _, _, scope, target, after_seq, limit = _REQUEST.unpack(raw)

        return SyncRequest(
            scope=scope,
            target_id=target,
            after_seq=after_seq,
            limit=limit,
        )

    if kind == SYNC_END:
        if len(raw) != _END.size:
            raise ProtocolError("invalid SyncEnd length")

        _, _, scope, target, last_seq, has_more = _END.unpack(raw)

        if has_more not in (0, 1):
            raise ProtocolError("invalid has_more")

        return SyncEnd(
            scope=scope,
            target_id=target,
            last_seq=last_seq,
            has_more=bool(has_more),
        )

    raise ProtocolError("unknown message type")