import sqlite3
import time
from pathlib import Path
from threading import RLock
from uuid import UUID, uuid4
from typing import Union, List, Tuple
from .models import (
    DIRECT,
    GROUP,
    MAX_SEQUENCE,
    MAX_SYNC_BATCH,
    MAX_U32,
    ChatMessage,
    ProtocolError,
    check_scope,
    require_int,
)


class MessageConflict(ValueError):
    pass


class MessageStore:
    def __init__(self, db_path: Union[str, Path], local_peer_id: int) -> None:
        require_int("local_peer_id", local_peer_id, 1, MAX_U32)
        self.local_peer_id = local_peer_id
        self._lock = RLock()

        db_path = Path(db_path)
        if str(db_path) != ":memory:":
            db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), timeout=5, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout = 5000")
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                sender_id    INTEGER NOT NULL,
                msg_id       BLOB NOT NULL CHECK(length(msg_id) = 16),
                kind         INTEGER NOT NULL,
                target_id    INTEGER NOT NULL,
                sender_seq   INTEGER NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                text         TEXT NOT NULL,
                PRIMARY KEY (sender_id, msg_id),
                UNIQUE (sender_id, kind, target_id, sender_seq)
            );

            CREATE TABLE IF NOT EXISTS sender_counters (
                kind       INTEGER NOT NULL,
                target_id  INTEGER NOT NULL,
                last_seq   INTEGER NOT NULL,
                PRIMARY KEY (kind, target_id)
            );

            CREATE INDEX IF NOT EXISTS idx_messages_sync
                ON messages(sender_id, kind, target_id, sender_seq);

            CREATE INDEX IF NOT EXISTS idx_messages_history
                ON messages(kind, target_id, timestamp_ms);

            CREATE TRIGGER IF NOT EXISTS no_update_messages
            BEFORE UPDATE ON messages
            BEGIN
                SELECT RAISE(ABORT, 'chat messages are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS no_delete_messages
            BEFORE DELETE ON messages
            BEGIN
                SELECT RAISE(ABORT, 'chat messages are immutable');
            END;
        """)

    @staticmethod
    def _row_to_message(row: sqlite3.Row) -> ChatMessage:
        return ChatMessage(
            kind=row["kind"],
            msg_id=UUID(bytes=row["msg_id"]),
            sender_id=row["sender_id"],
            target_id=row["target_id"],
            sender_seq=row["sender_seq"],
            timestamp_ms=row["timestamp_ms"],
            text=row["text"],
        )

    def _insert(self, message: ChatMessage) -> None:
        self._conn.execute(
            """INSERT INTO messages
               (sender_id, msg_id, kind, target_id, sender_seq, timestamp_ms, text)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                message.sender_id,
                message.msg_id.bytes,
                message.kind,
                message.target_id,
                message.sender_seq,
                message.timestamp_ms,
                message.text,
            ),
        )

    def create_outgoing(self, kind: int, target_id: int, text: str) -> ChatMessage:
        """Assign the local sender's next sequence and store the message atomically."""
        check_scope(kind, target_id)
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                row = self._conn.execute(
                    "SELECT last_seq FROM sender_counters WHERE kind=? AND target_id=?",
                    (kind, target_id),
                ).fetchone()
                seq = row["last_seq"] + 1 if row else 1
                if seq > MAX_SEQUENCE:
                    raise ProtocolError("sender_seq exhausted")
                message = ChatMessage(
                    kind=kind,
                    msg_id=uuid4(),
                    sender_id=self.local_peer_id,
                    target_id=target_id,
                    sender_seq=seq,
                    timestamp_ms=int(time.time() * 1000),
                    text=text,
                )
                self._insert(message)
                self._conn.execute(
                    """INSERT INTO sender_counters(kind, target_id, last_seq)
                       VALUES (?, ?, ?)
                       ON CONFLICT(kind, target_id) DO UPDATE
                       SET last_seq=excluded.last_seq""",
                    (kind, target_id, seq),
                )
                self._conn.commit()
                return message
            except Exception:
                self._conn.rollback()
                raise

    def save_incoming(self, message: ChatMessage) -> bool:
        """Return True on a new insert, False for an identical duplicate."""
        if not isinstance(message, ChatMessage):
            raise TypeError("message must be ChatMessage")
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                existing = self._conn.execute(
                    "SELECT * FROM messages WHERE sender_id=? AND msg_id=?",
                    (message.sender_id, message.msg_id.bytes),
                ).fetchone()
                if existing is not None:
                    if self._row_to_message(existing) != message:
                        raise MessageConflict("message ID reused with different content")
                    self._conn.commit()
                    return False

                other = self._conn.execute(
                    """SELECT 1 FROM messages
                       WHERE sender_id=? AND kind=? AND target_id=? AND sender_seq=?""",
                    (message.sender_id, message.kind, message.target_id, message.sender_seq),
                ).fetchone()
                if other is not None:
                    raise MessageConflict("author sequence reused with different message ID")
                self._insert(message)
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def last_contiguous(self, sender_id: int, kind: int, target_id: int) -> int:
        """Return the largest sequence prefix 1..N with no gaps."""
        require_int("sender_id", sender_id, 1, MAX_U32)
        check_scope(kind, target_id)
        with self._lock:
            rows = self._conn.execute(
                """SELECT sender_seq FROM messages
                   WHERE sender_id=? AND kind=? AND target_id=?
                   ORDER BY sender_seq ASC""",
                (sender_id, kind, target_id),
            )
            last = 0
            for row in rows:
                if row["sender_seq"] != last + 1:
                    break
                last += 1
            return last

    def authored_after(
        self, kind: int, target_id: int, after_seq: int, limit: int = MAX_SYNC_BATCH
    ) -> Tuple[List[ChatMessage], bool]:
        """Replay the local peer's messages; report whether another page exists."""
        check_scope(kind, target_id)
        require_int("after_seq", after_seq, 0, MAX_SEQUENCE)
        require_int("limit", limit, 1, MAX_SYNC_BATCH)
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM messages
                   WHERE sender_id=? AND kind=? AND target_id=? AND sender_seq>?
                   ORDER BY sender_seq ASC LIMIT ?""",
                (self.local_peer_id, kind, target_id, after_seq, limit + 1),
            ).fetchall()
        has_more = len(rows) > limit
        return [self._row_to_message(row) for row in rows[:limit]], has_more

    def history(self, kind: int, target_id: int, limit: int = 50) -> list[ChatMessage]:
        """Fetch recent direct conversation or group history, oldest to newest."""
        check_scope(kind, target_id)
        require_int("limit", limit, 1, 1000)
        with self._lock:
            if kind == DIRECT:
                rows = self._conn.execute(
                    """SELECT * FROM messages
                       WHERE kind=? AND (
                          (sender_id=? AND target_id=?) OR
                          (sender_id=? AND target_id=?)
                       )
                       ORDER BY timestamp_ms DESC, sender_seq DESC LIMIT ?""",
                    (DIRECT, self.local_peer_id, target_id,
                     target_id, self.local_peer_id, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """SELECT * FROM messages
                       WHERE kind=? AND target_id=?
                       ORDER BY timestamp_ms DESC, sender_seq DESC LIMIT ?""",
                    (GROUP, target_id, limit),
                ).fetchall()
        return [self._row_to_message(row) for row in reversed(rows)]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
