"""Run agent turns as local child workflows.

An agent workflow runs a turn with :func:`run_local_turn`, which starts the turn's workflow as an
ordinary child workflow on the task queue :data:`LOCAL_TURNS_TASK_QUEUE`. A worker process serves
that task queue with :class:`LocalTurns`: an in-process Temporal server (sdk-python's
``local_server_module``) that takes ownership of each new turn from the server and runs it in the
process, so that the turn's model and tool calls make no calls to the server. It sends the turn's
history to the server at each sync interval and when the turn closes. The server must support local
execution (temporalio/temporal branch sj/local-first-execution).

The local server keeps its state in memory. If the worker process dies, the server takes the turn
back once its ownership expires (three sync intervals), and the turn can then continue from its last
synced history only on a worker connected to the server itself: the local server can take ownership
of new turns only.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from types import TracebackType
from typing import Any, Self

from temporalio import workflow
from temporalio.client import Client, Plugin
from temporalio.service import LocalServerUpstream
from temporalio.worker import Worker

LOCAL_TURNS_TASK_QUEUE = "local-turns"


async def run_local_turn(turn_workflow: str, input: Any, *, result_type: type) -> Any:
    """Runs a turn as a child workflow on the local turns task queue. Call from a workflow."""
    return await workflow.execute_child_workflow(
        turn_workflow,
        input,
        id=f"{workflow.info().workflow_id}-turn-{workflow.uuid4()}",
        task_queue=LOCAL_TURNS_TASK_QUEUE,
        result_type=result_type,
    )


class LocalTurns:
    """A local server whose turns the server at ``upstream`` owns, and a worker that runs turn
    workflows on it, for the lifetime of the ``async with`` block."""

    def __init__(
        self,
        module: str,
        upstream: str,
        *,
        workflows: Sequence[type],
        activities: Sequence[Callable] = (),
        plugins: Sequence[Plugin] = (),
    ) -> None:
        """``module`` is the path of a precompiled local-server module (.cwasm); ``upstream`` is
        the ``host:port`` of the server."""
        self._module = module
        self._upstream = upstream
        self._workflows = workflows
        self._activities = activities
        self._plugins = plugins

    async def __aenter__(self) -> Self:
        self.client = await Client.connect(
            "local",
            local_server_module=self._module,
            local_server_upstream=LocalServerUpstream(target_host=self._upstream),
            plugins=self._plugins,
        )
        self._worker = Worker(
            self.client,
            task_queue=LOCAL_TURNS_TASK_QUEUE,
            workflows=self._workflows,
            activities=self._activities,
        )
        await self._worker.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._worker.__aexit__(exc_type, exc, tb)
