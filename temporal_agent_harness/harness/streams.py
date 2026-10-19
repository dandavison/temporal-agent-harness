# ABOUTME: The agent's event stream, stored by a ``temporalio.streams`` backend: the backend
# each client uses, the batched writer an activity attempt appends with, and the reader the
# client merge and the subagent activity consume.
#
# DESIGN — one backend per Client: ``AgentHarnessPlugin(stream_backend=...)`` takes a factory,
# because ``NativeStreamBackend.connect`` needs a connected Client, which does not exist when the
# plugin is constructed. The backend connects lazily, once per Client. A worker's plugin binds it
# to the worker's Client; a Client with the plugin and no worker (a BFF) gets it from its plugins.
#
# DESIGN — cursors: a position is the backend's opaque cursor, base64-encoded so it travels in
# Pydantic models, JSON and URLs. ``BEGINNING`` ("") reads from the start. A cursor is a resume
# point: reading after it returns the records that follow. Cursors cannot be ordered, only
# compared for equality.

from __future__ import annotations

import asyncio
import base64
import os
import weakref
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeVar

import temporalio.api.protocol.v1
from temporalio import activity
from temporalio.client import Client
from temporalio.streams import (
    ActivityProducer,
    AppendRecord,
    Producer,
    ReadResponse,
    StreamBackend,
    StreamClose,
    StreamRef,
    StreamWriter,
    WorkflowActivationCompleted,
    WorkflowProducer,
    decode,
)

T = TypeVar("T")

StreamBackendFactory = Callable[[Client], Awaitable[StreamBackend]]

BEGINNING = ""

# How long one read waits for a record before reading again. Bounds how long a reader takes to
# notice that it was cancelled server-side; it is not a latency floor, since a read returns as
# soon as a record arrives.
_READ_WAIT = timedelta(seconds=20)


def native_streams(*, commit_mode: bool = False) -> StreamBackendFactory:
    """Streams stored by the Temporal server."""

    async def connect(client: Client) -> StreamBackend:
        from temporalio.streams.native import NativeStreamBackend

        return await NativeStreamBackend.connect(client, commit_mode=commit_mode)

    return connect


def redis_streams(url: str, *, retention: timedelta | None = None) -> StreamBackendFactory:
    """Streams stored in Redis, namespaced by the Client's namespace."""

    async def connect(client: Client) -> StreamBackend:
        from temporalio.streams.redis import RedisStreamBackend

        if retention is None:
            return await RedisStreamBackend.connect(url, client.namespace)
        return await RedisStreamBackend.connect(url, client.namespace, retention=retention)

    return connect


def stream_backend_from_env() -> StreamBackendFactory:
    """The backend named by ``TEMPORAL_AGENT_STREAM_BACKEND``: ``native`` (the default),
    ``native-commit``, or ``redis`` at ``TEMPORAL_AGENT_STREAM_REDIS_URL``."""
    name = os.environ.get("TEMPORAL_AGENT_STREAM_BACKEND", "native")
    if name == "native":
        return native_streams()
    if name == "native-commit":
        return native_streams(commit_mode=True)
    if name == "redis":
        return redis_streams(os.environ["TEMPORAL_AGENT_STREAM_REDIS_URL"])
    raise ValueError(f"unknown TEMPORAL_AGENT_STREAM_BACKEND {name!r}")


_backends: weakref.WeakKeyDictionary[Client, StreamBackend] = weakref.WeakKeyDictionary()


def stream_backend(client: Client) -> StreamBackend:
    """The stream backend bound to ``client``, or else that of its ``AgentHarnessPlugin``."""
    backend = _backends.get(client)
    if backend is None:
        from temporal_agent_harness.plugin import AgentHarnessPlugin

        plugins = [p for p in client.config()["plugins"] if isinstance(p, AgentHarnessPlugin)]
        if not plugins:
            raise RuntimeError(
                "the Client has no AgentHarnessPlugin and no worker with one, so it has no "
                "stream backend"
            )
        backend = bind_stream_backend(client, plugins[0].stream_backend_factory)
    return backend


def bind_stream_backend(client: Client, factory: StreamBackendFactory) -> StreamBackend:
    """Bind ``client`` to a backend from ``factory``, unless it already has one."""
    backend = _backends.get(client)
    if backend is None:
        backend = _backends[client] = _LazyStreamBackend(factory, client)
    return backend


def encode_cursor(cursor: bytes | None) -> str:
    return base64.urlsafe_b64encode(cursor).decode() if cursor else BEGINNING


def decode_cursor(cursor: str) -> bytes | None:
    return base64.urlsafe_b64decode(cursor) if cursor else None


def workflow_ref(workflow_id: str) -> StreamRef:
    return StreamRef(workflow_id=workflow_id)


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
    """The cursor of the last data record of ``topic``, or ``BEGINNING`` if it has none.

    Native markers match every topic and count towards ``last``, so this reads further back
    until it finds a data record.
    """
    backend = stream_backend(client)
    last = 4
    while True:
        resp = await backend.read(workflow_ref(workflow_id), topics=[topic], last=last)
        data = [r for r in resp.records if r.payload is not None]
        if data:
            return encode_cursor(data[-1].cursor)
        if len(resp.records) < last:
            return BEGINNING
        last *= 4


async def subscribe(
    client: Client,
    workflow_id: str,
    *,
    topic: str,
    result_type: type[T],
    after: str = BEGINNING,
    mark: str | None = None,
) -> AsyncIterator[StreamItem[T]]:
    """Yield the data records of ``topic`` after ``after``, live, until the stream closes.

    Markers and producer finishes are skipped. ``mark`` is a cursor from :func:`head`: each item
    says whether it is at or after it.
    """
    backend = stream_backend(client)
    ref = workflow_ref(workflow_id)
    position = decode_cursor(after)
    reached = mark is None or mark in (BEGINNING, after)
    while True:
        resp = await backend.read(ref, after=position, topics=[topic], wait=_READ_WAIT)
        for record in resp.records:
            cursor = encode_cursor(record.cursor)
            reached = reached or cursor == mark
            if record.payload is None:
                continue
            data = await decode(record, result_type, client.data_converter)
            yield StreamItem(data=data, cursor=cursor, reached_mark=reached)
        if resp.close is not None:
            return
        position = resp.cursor or position


# ---------------------------------------------------------------------------
# Writing from an activity
# ---------------------------------------------------------------------------


class ActivityStreamPublisher:
    """Appends values to the stream of the workflow that scheduled the current activity attempt,
    batched on a background flush.

    Every publisher of one attempt is the same producer and epoch, which ``temporalio.streams``
    requires to share one writer, so the attempt's publishers share this one: get it with
    :func:`activity_publisher`.
    """

    def __init__(self, writer: StreamWriter, batch_interval: timedelta) -> None:
        self._writer = writer
        self._batch_interval = batch_interval
        self._pending: list[tuple[str, Any]] = []
        self._users = 0
        self._flusher: asyncio.Task[None] | None = None

    def publish(self, value: Any, *, topic: str) -> None:
        self._pending.append((topic, value))

    async def _flush(self) -> None:
        pending, self._pending = self._pending, []
        for topic, values in _runs_by_topic(pending):
            await self._writer.append(*values, topic=topic)

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
async def activity_publisher(batch_interval: timedelta) -> AsyncIterator[ActivityStreamPublisher]:
    """The current activity attempt's publisher, drained when its last user exits."""
    info = activity.info()
    publisher = _publishers.get(info.task_token)
    if publisher is None:
        client = activity.client()
        # The harness never continues as new, so the run that scheduled the activity is its
        # chain's first run, which a Redis append needs.
        ref = StreamRef(info.workflow_id or "", info.workflow_run_id or "")
        writer = StreamWriter.for_activity(stream_backend(client), ref, client.data_converter)
        publisher = _publishers[info.task_token] = ActivityStreamPublisher(writer, batch_interval)
    await publisher._enter()
    try:
        yield publisher
    finally:
        if await publisher._exit():
            del _publishers[info.task_token]


def _runs_by_topic(pending: Sequence[tuple[str, Any]]) -> list[tuple[str, list[Any]]]:
    """Consecutive values of one topic, since one append carries one topic."""
    runs: list[tuple[str, list[Any]]] = []
    for topic, value in pending:
        if runs and runs[-1][0] == topic:
            runs[-1][1].append(value)
        else:
            runs.append((topic, [value]))
    return runs


# ---------------------------------------------------------------------------
# Lazy backend
# ---------------------------------------------------------------------------


class _LazyStreamBackend(StreamBackend):
    """Connects to the backend on first use, so it can be created where awaiting is impossible
    (a plugin's ``configure_worker``)."""

    def __init__(self, factory: StreamBackendFactory, client: Client) -> None:
        self._factory = factory
        self._client = client
        self._backend: StreamBackend | None = None
        self._lock = asyncio.Lock()

    async def _connected(self) -> StreamBackend:
        async with self._lock:
            if self._backend is None:
                self._backend = await self._factory(self._client)
            return self._backend

    async def append(
        self,
        ref: StreamRef,
        producer: Producer,
        seq: int,
        records: Sequence[AppendRecord],
        *,
        finish: bool = False,
    ) -> Sequence[bytes]:
        return await (await self._connected()).append(ref, producer, seq, records, finish=finish)

    async def close(
        self,
        ref: StreamRef,
        owner: WorkflowProducer | ActivityProducer,
        seq: int,
        close: StreamClose,
    ) -> None:
        await (await self._connected()).close(ref, owner, seq, close)

    async def read(
        self,
        ref: StreamRef,
        *,
        after: bytes | None = None,
        topics: Sequence[str] = (),
        max_records: int = 0,
        wait: timedelta = timedelta(0),
        last: int = 0,
    ) -> ReadResponse:
        return await (await self._connected()).read(
            ref, after=after, topics=topics, max_records=max_records, wait=wait, last=last
        )

    async def workflow_activation_completed(
        self, completed: WorkflowActivationCompleted
    ) -> Sequence[temporalio.api.protocol.v1.Message]:
        return await (await self._connected()).workflow_activation_completed(completed)

    async def workflow_evicted(self, run_id: str, *, execution_ending: bool) -> None:
        await (await self._connected()).workflow_evicted(run_id, execution_ending=execution_ending)


__all__ = [
    "BEGINNING",
    "ActivityStreamPublisher",
    "StreamBackendFactory",
    "StreamItem",
    "activity_publisher",
    "bind_stream_backend",
    "head",
    "native_streams",
    "redis_streams",
    "stream_backend",
    "stream_backend_from_env",
    "subscribe",
]
