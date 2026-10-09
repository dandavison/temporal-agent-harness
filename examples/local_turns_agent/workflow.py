"""An agent that answers questions about the files in the directory it runs in, whose turns run
locally as child workflows.

The agent workflow runs on the server and keeps the conversation; the harness runner admits
messages and publishes turn events as usual. Each turn is a child workflow, ``FilesTurn``, on
the task queue ``TURN_TASK_QUEUE``, whose worker has local execution: the OpenAI Agents SDK loop,
with its model and tool calls as activities of the turn, runs in that worker's process, and the
server shows the turn's history as the worker syncs it.

``LocalTurnsNexusAgentWorkflow`` is the same agent, except that it starts each turn through a Nexus
operation, ``TurnService.run_turn``, whose handler starts ``FilesTurn`` on the same task queue.

Compared with running the loop in the agent workflow, a turn loses the harness's tool approvals,
callback tools, tool events and streamed reply deltas: those go through the agent workflow, which
the turn cannot reach.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import nexusrpc
from temporalio import activity, nexus, workflow
from temporalio.contrib.workflow_streams import WorkflowStream
from temporalio.nexus import WorkflowRunOperationContext, workflow_run_operation

with workflow.unsafe.imports_passed_through():
    from agents import Agent, Runner
    from pydantic import BaseModel

    from temporal_agent_harness.ai_sdks.openai_agents.workflow import activity_as_tool
    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        TextMessage,
        TextReply,
        ToolApprovalPolicy,
    )
    from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner

MODEL = "gpt-5.6-luna"
TURN_TASK_QUEUE = "local-turns"
TURN_ENDPOINT = "local-turns"
"""The Nexus endpoint for ``TurnService``; its target is ``TURN_HANDLER_TASK_QUEUE``."""
TURN_HANDLER_TASK_QUEUE = "local-turns-handler"
MAX_FILE_CHARS = 20_000


@activity.defn
async def list_files(directory: str) -> list[str]:
    """List the files and directories in a directory, given relative to the project root ("."
    is the root). Directory names end with "/"."""
    path = _project_path(directory)
    if path is None or not path.is_dir():
        return [f"error: {directory} is not a directory in the project"]
    return sorted(
        f"{p.name}/" if p.is_dir() else p.name
        for p in path.iterdir()
        if not p.name.startswith(".")
    )


@activity.defn
async def read_file(file: str) -> str:
    """Read a file, given relative to the project root. Returns at most the first 20,000
    characters."""
    path = _project_path(file)
    if path is None or not path.is_file():
        return f"error: {file} is not a file in the project"
    return path.read_text(errors="replace")[:MAX_FILE_CHARS]


def _project_path(relative: str) -> Path | None:
    """The path of ``relative`` within the project root, the worker's working directory, or None
    if it is outside the root."""
    root = Path.cwd().resolve()
    path = (root / relative).resolve()
    return path if path.is_relative_to(root) else None


class TurnInput(BaseModel):
    conversation: list[dict[str, Any]]
    text: str


class TurnOutput(BaseModel):
    conversation: list[dict[str, Any]]
    reply: str


@workflow.defn
class FilesTurn:
    """One turn, run locally as a child workflow."""

    @workflow.run
    async def run(self, turn: TurnInput) -> TurnOutput:
        files_agent = Agent(
            name="Files",
            instructions=(
                "Answer questions about the files in the project. Use list_files and "
                "read_file to look at them; paths are relative to the project root."
            ),
            model=MODEL,
            tools=[
                activity_as_tool(tool, start_to_close_timeout=timedelta(seconds=30))
                for tool in (list_files, read_file)
            ],
        )
        result = await Runner.run(
            files_agent,
            input=[*turn.conversation, {"role": "user", "content": turn.text}],
        )
        return TurnOutput(
            conversation=result.to_input_list(), reply=str(result.final_output)
        )


@agent.defn(name="LocalTurnsAgent")
class LocalTurnsAgentWorkflow:
    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
        )
        self._conversation: list[dict[str, Any]] = []

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Ask about the files in the project."""
        output = await workflow.execute_child_workflow(
            FilesTurn.run,
            TurnInput(conversation=self._conversation, text=message.text),
            id=f"{workflow.info().workflow_id}-turn-{workflow.uuid4()}",
            task_queue=TURN_TASK_QUEUE,
        )
        self._conversation = output.conversation
        return TextReply(text=output.reply)


@nexusrpc.handler.service_handler
class TurnService:
    @workflow_run_operation
    async def run_turn(
        self, ctx: WorkflowRunOperationContext, turn: TurnInput
    ) -> nexus.WorkflowHandle[TurnOutput]:
        return await ctx.start_workflow(
            FilesTurn.run, turn, id=f"turn-{uuid.uuid4()}", task_queue=TURN_TASK_QUEUE
        )


@agent.defn(name="LocalTurnsNexusAgent")
class LocalTurnsNexusAgentWorkflow:
    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
        )
        self._conversation: list[dict[str, Any]] = []
        self._turns = workflow.create_nexus_client(
            service=TurnService, endpoint=TURN_ENDPOINT
        )

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Ask about the files in the project."""
        output = await self._turns.execute_operation(
            TurnService.run_turn,
            TurnInput(conversation=self._conversation, text=message.text),
        )
        self._conversation = output.conversation
        return TextReply(text=output.reply)
