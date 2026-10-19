# ABOUTME: The Temporal server and stream backend the tests run against. ``--stream-backend``
# chooses the backend: ``native`` (the default), ``native-commit`` or ``redis``. Every backend
# needs the stream prototype's server (the superproject's ``dev-server``) at
# ``TEMPORAL_ADDRESS`` (default localhost:7233): native streams are stored there, and the Redis
# backend needs the attempt stamps it sets. Redis needs ``redis-server`` on PATH.
#
# The tests share one server, so unlike the time-skipping test server it replaces, timers take
# real time and every test must use unique workflow IDs.

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass

from temporalio.client import Client, Plugin
from temporalio.converter import DataConverter

from temporal_agent_harness.harness.streams import (
    StreamBackendFactory,
    native_streams,
    redis_streams,
)
from temporal_agent_harness.plugin import AgentHarnessPlugin

BACKENDS = ("native", "native-commit", "redis")

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")

# Set by tests/conftest.py from the command line.
backend = "native"
redis_url = ""


def stream_backend_factory() -> StreamBackendFactory:
    if backend == "native":
        return native_streams()
    if backend == "native-commit":
        return native_streams(commit_mode=True)
    return redis_streams(redis_url)


@dataclass
class StreamEnv:
    """Stands in for ``WorkflowEnvironment``: a client of the shared server."""

    client: Client

    async def shutdown(self) -> None:
        pass

    async def __aenter__(self) -> StreamEnv:
        return self

    async def __aexit__(self, *exc: object) -> None:
        pass


async def start_env(
    *,
    data_converter: DataConverter = DataConverter.default,
    plugins: Sequence[Plugin] = (),
) -> StreamEnv:
    """A client with an ``AgentHarnessPlugin`` on the chosen backend, unless ``plugins`` has one.

    The added plugin does not offload large payloads, so a test's data converter is used as given.
    """
    if not any(isinstance(p, AgentHarnessPlugin) for p in plugins):
        plugins = [
            *plugins,
            AgentHarnessPlugin(large_payload_offload=None, stream_backend=stream_backend_factory()),
        ]
    client = await Client.connect(
        TEMPORAL_ADDRESS, namespace="default", data_converter=data_converter, plugins=plugins
    )
    return StreamEnv(client)
