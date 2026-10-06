"""Run agent turns as local workflows.

A local workflow runs in this worker process against an in-process Temporal server (sdk-python's
``local_server_module``), so the model and tool calls of a turn are activities that make no calls
to a real server. A server-hosted agent workflow runs a turn by executing the activity
``LocalTurns.run_turn``, which runs the turn's workflow locally and returns its result and history.

The local server keeps its state in memory: if the worker process dies, the turn is lost and runs
again from the start under the activity's retry policy.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

from temporalio import activity
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.workflowservice.v1 import DeleteWorkflowExecutionRequest
from temporalio.client import Client, Plugin
from temporalio.worker import Worker


@dataclass
class LocalTurn:
    """A turn to run as a local workflow."""

    workflow: str
    """Name of the workflow type that runs the turn."""
    input: Any


@dataclass
class LocalTurnResult:
    output: Any
    history: dict[str, Any]
    """The local workflow's history, in Temporal's JSON history format."""


class LocalTurns:
    """A local server and a worker that runs turn workflows on it, for the lifetime of the
    ``async with`` block."""

    _task_queue = "local-turns"

    def __init__(
        self,
        module: str,
        *,
        workflows: Sequence[type],
        activities: Sequence[Callable] = (),
        plugins: Sequence[Plugin] = (),
    ) -> None:
        """``module`` is the path of a precompiled local-server module (.cwasm)."""
        self._module = module
        self._workflows = workflows
        self._activities = activities
        self._plugins = plugins

    async def __aenter__(self) -> Self:
        self.client = await Client.connect(
            "local", local_server_module=self._module, plugins=self._plugins
        )
        self._worker = Worker(
            self.client,
            task_queue=self._task_queue,
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

    @activity.defn(name="run_local_turn")
    async def run_turn(self, turn: LocalTurn) -> LocalTurnResult:
        """Runs the turn's workflow, then deletes it from the local server."""
        handle = await self.client.start_workflow(
            turn.workflow,
            turn.input,
            id=f"turn-{uuid.uuid4()}",
            task_queue=self._task_queue,
        )
        output = await handle.result()
        history = await handle.fetch_history()
        await self.client.workflow_service.delete_workflow_execution(
            DeleteWorkflowExecutionRequest(
                namespace=self.client.namespace,
                workflow_execution=WorkflowExecution(workflow_id=handle.id),
            )
        )
        return LocalTurnResult(output=output, history=history.to_json_dict())
