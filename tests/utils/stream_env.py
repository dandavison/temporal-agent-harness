# ABOUTME: The Temporal server and stream backend the tests run against. ``--stream-backend``
# chooses the backend: ``native`` (the default), ``native-commit`` or ``redis``. Every backend
# needs the stream prototype's server (the superproject's ``dev-server``) at
# ``TEMPORAL_ADDRESS`` (default localhost:7233): native streams are stored there, and the Redis
# backend needs the attempt stamps it sets. Redis needs ``redis-server`` on PATH.
#
# Commit mode is declared by a workflow definition, ``@workflow.defn(stream_commit_mode=True)``,
# not chosen by a backend. So that one suite runs in both native modes, ``native-commit`` adds a
# test-only worker plugin that redeclares every workflow a worker registers as commit mode,
# rather than every test workflow having a definition for each mode. The plugin replaces the
# definition on the class, which the worker uses for the append mode; a sandboxed workflow
# re-imports its module and gets the original definition, which only a workflow stream's
# ``close()`` and its publishes' choice of whether to wait for the store consult. The harness does
# not close its stream, and waiting on the store is correct in either mode.
#
# The tests share one server, so unlike the time-skipping test server it replaces, timers take
# real time and every test must use unique workflow IDs.

from __future__ import annotations

import dataclasses
import os
from collections.abc import Sequence
from dataclasses import dataclass

from temporalio.client import Client, Plugin
from temporalio.contrib.redis_streams import RedisStreamBackend
from temporalio.converter import DataConverter
from temporalio.plugin import SimplePlugin
from temporalio.streams import StreamBackends
from temporalio.worker import WorkerConfig
from temporalio.workflow import _Definition

from temporal_agent_harness.plugin import AgentHarnessPlugin

BACKENDS = ("native", "native-commit", "redis")

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")

# Set by tests/conftest.py from the command line.
backend = "native"
redis_url = ""


def stream_plugins() -> list[Plugin]:
    if backend == "native-commit":
        return [CommitModePlugin()]
    return []


def stream_backends() -> StreamBackends | None:
    if backend == "redis":
        return StreamBackends([RedisStreamBackend(redis_url)])
    return None


class CommitModePlugin(SimplePlugin):
    """Redeclares every workflow a worker registers as ``stream_commit_mode=True``."""

    def __init__(self) -> None:
        super().__init__(name="CommitModePlugin")

    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        config = super().configure_worker(config)
        for cls in config.get("workflows") or ():
            defn = dataclasses.replace(_Definition.must_from_class(cls), stream_commit_mode=True)
            setattr(cls, "__temporal_workflow_definition", defn)
            setattr(defn.run_fn, "__temporal_workflow_definition", defn)
        return config


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
    """A client on the chosen backend, with an ``AgentHarnessPlugin`` unless ``plugins`` has one.

    The added plugin does not offload large payloads, so a test's data converter is used as given.
    """
    if not any(isinstance(p, AgentHarnessPlugin) for p in plugins):
        plugins = [*plugins, AgentHarnessPlugin(large_payload_offload=None)]
    plugins = [*plugins, *stream_plugins()]
    client = await Client.connect(
        TEMPORAL_ADDRESS,
        namespace="default",
        data_converter=data_converter,
        plugins=plugins,
        stream_backends=stream_backends(),
    )
    return StreamEnv(client)
