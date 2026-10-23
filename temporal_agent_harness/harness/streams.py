# ABOUTME: The agent's event stream, stored by ``temporalio.streams``: the backend choice for
# the example stack, the batched publisher an activity attempt appends with, and the reader the
# client merge and the subagent activity consume.
#
# DESIGN — backend: a client stores streams natively unless its stream backends configuration
# names another default, and its workers use the client's. ``AgentHarnessPlugin`` makes Redis the
# client's default backend when ``TEMPORAL_AGENT_STREAM_BACKEND=redis``, so the example stack's
# clients and workers agree.
#
# DESIGN — the stream: each agent workflow owns one stream, named ``EVENTS_STREAM``, which its run
# creates. An activity writes to its workflow's stream through the handle that the workflow
# threads to it in ``TurnStreamContext``.
#
# DESIGN — cursors: a position is the stream's opaque cursor, base64-encoded so it travels in
# Pydantic models, JSON and URLs. ``BEGINNING`` ("") reads from the start. A cursor is a resume
# point: reading after it returns the records that follow. Cursors cannot be ordered, only
# compared for equality.

from __future__ import annotations

import asyncio
import base64
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeVar

from temporalio import activity
from temporalio.client import Client
from temporalio.contrib.redis_streams import RedisStreamBackend
from temporalio.streams import ActivityStreamHandle, StreamBackends, StreamCursor, StreamRecord

from temporal_agent_harness.harness.stream_context import TurnStreamContext

T = TypeVar("T")

BEGINNING = ""

EVENTS_STREAM = "events"
"""The name of the stream an agent workflow owns."""


def redis_backends_from_env() -> StreamBackends | None:
    """Stream backends with Redis at ``TEMPORAL_AGENT_STREAM_REDIS_URL`` if
    ``TEMPORAL_AGENT_STREAM_BACKEND`` is ``redis``, and ``None`` if it is ``native`` (the
    default)."""
    name = os.environ.get("TEMPORAL_AGENT_STREAM_BACKEND", "native")
    if name == "native":
        return None
    if name == "redis":
        return StreamBackends(
            [RedisStreamBackend(os.environ["TEMPORAL_AGENT_STREAM_REDIS_URL"])], default="redis"
        )
    raise ValueError(f"unknown TEMPORAL_AGENT_STREAM_BACKEND {name!r}")


def encode_cursor(cursor: bytes | None) -> str:
    return base64.urlsafe_b64encode(cursor).decode() if cursor else BEGINNING


def decode_cursor(cursor: str) -> StreamCursor | None:
    return StreamCursor(base64.urlsafe_b64decode(cursor)) if cursor else None


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StreamItem(Generic[T]):
    """A decoded data record, and the cursor to read after it from."""

    data: T
    cursor: str
    # Whether this record is at or after the ``mark`` given to :func:`subscribe`.
    reached_mark: bool


async def head(client: Client, workflow_id: str, *, topic: str) -> str:
    """The cursor of the last record of ``topic``, or ``BEGINNING`` if it has none."""
    stream = client.get_stream_handle(workflow_id=workflow_id, name=EVENTS_STREAM)
    last = await stream.fetch_last_records(1, topics=[topic])
    return encode_cursor(last[-1].cursor) if last else BEGINNING


async def subscribe(
    client: Client,
    workflow_id: str,
    *,
    topic: str,
    result_type: type[T],
    after: str = BEGINNING,
    mark: str | None = None,
) -> AsyncIterator[StreamItem[T]]:
    """Yield the records of ``topic`` after ``after``, live, until the stream closes.

    Withdrawal notices are skipped, so a consumer keeps a record that is withdrawn after it was
    yielded. ``mark`` is a cursor from :func:`head`: each item says whether it is at or after it.
    """
    stream = client.get_stream_handle(workflow_id=workflow_id, name=EVENTS_STREAM)
    reached = mark is None or mark in (BEGINNING, after)
    async for record in stream.fetch_records(
        after=decode_cursor(after),
        topics=[topic],
        wait_new_records=True,
        type_hint=result_type,
    ):
        if not isinstance(record, StreamRecord):
            continue
        cursor = encode_cursor(record.cursor)
        reached = reached or cursor == mark
        yield StreamItem(data=record.value, cursor=cursor, reached_mark=reached)


# ---------------------------------------------------------------------------
# Writing from an activity
# ---------------------------------------------------------------------------


class ActivityStreamPublisher:
    """Publishes values to the stream of the workflow that scheduled the current activity
    attempt, appending what was published once per ``batch_interval``, one append per run of
    values with the same topic.

    The attempt's publishers share one, so that values reach the stream in publish order: get it
    with :func:`activity_publisher`.
    """

    def __init__(self, stream: ActivityStreamHandle, batch_interval: timedelta) -> None:
        self._stream = stream
        self._batch_interval = batch_interval
        self._pending: list[tuple[str, Any]] = []
        self._users = 0
        self._flusher: asyncio.Task[None] | None = None

    def publish(self, value: Any, *, topic: str) -> None:
        self._pending.append((topic, value))

    async def _flush(self) -> None:
        pending, self._pending = self._pending, []
        start = 0
        for end in range(1, len(pending) + 1):
            if end == len(pending) or pending[end][0] != pending[start][0]:
                await self._stream.publish(
                    *(v for _, v in pending[start:end]), topic=pending[start][0]
                )
                start = end

    async def _flush_periodically(self) -> None:
        while True:
            await asyncio.sleep(self._batch_interval.total_seconds())
            await self._flush()

    async def _enter(self) -> None:
        self._users += 1
        if self._flusher is None:
            self._flusher = asyncio.create_task(self._flush_periodically())

    async def _exit(self) -> bool:
        self._users -= 1
        if self._users:
            return False
        assert self._flusher is not None
        self._flusher.cancel()
        try:
            await self._flusher
        except asyncio.CancelledError:
            pass
        await self._flush()
        return True


_publishers: dict[bytes, ActivityStreamPublisher] = {}


@asynccontextmanager
async def activity_publisher(
    context: TurnStreamContext, batch_interval: timedelta
) -> AsyncIterator[ActivityStreamPublisher]:
    """The current activity attempt's publisher to the stream of the workflow that scheduled
    it, drained when its last user exits."""
    info = activity.info()
    publisher = _publishers.get(info.task_token)
    if publisher is None:
        stream = activity.get_stream_handle(
            workflow_id=info.workflow_id,
            first_execution_run_id=context.stream_first_run_id,
            name=EVENTS_STREAM,
        )
        publisher = _publishers[info.task_token] = ActivityStreamPublisher(
            stream, batch_interval
        )
    await publisher._enter()
    try:
        yield publisher
    finally:
        if await publisher._exit():
            del _publishers[info.task_token]


__all__ = [
    "BEGINNING",
    "EVENTS_STREAM",
    "ActivityStreamPublisher",
    "StreamItem",
    "activity_publisher",
    "head",
    "redis_backends_from_env",
    "subscribe",
]
