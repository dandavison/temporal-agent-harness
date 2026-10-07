"""A weather agent whose turns run locally as child workflows.

The agent workflow runs on the server and keeps the conversation; the harness runner admits
messages and publishes turn events as usual. Each turn is a child workflow, ``WeatherTurn``, on
the task queue ``TURN_TASK_QUEUE``, whose worker has local execution: the OpenAI Agents SDK loop,
with its model and tool calls as activities of the turn, runs in that worker's process, and the
server shows the turn's history as the worker syncs it.

Compared with running the loop in the agent workflow, a turn loses the harness's tool approvals,
callback tools, tool events and streamed reply deltas: those go through the agent workflow, which
the turn cannot reach.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.contrib.workflow_streams import WorkflowStream

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


@activity.defn
async def get_weather(city: str) -> str:
    """Get the weather for a city."""
    # Slow, so that the turn can be seen running in the Temporal UI.
    await asyncio.sleep(3)
    return f"sunny in {city}"


class TurnInput(BaseModel):
    conversation: list[dict[str, Any]]
    text: str


class TurnOutput(BaseModel):
    conversation: list[dict[str, Any]]
    reply: str


@workflow.defn
class WeatherTurn:
    """One turn, run locally as a child workflow."""

    @workflow.run
    async def run(self, turn: TurnInput) -> TurnOutput:
        weather_agent = Agent(
            name="Weather",
            instructions="Answer weather questions. Use get_weather.",
            model=MODEL,
            tools=[
                activity_as_tool(
                    get_weather, start_to_close_timeout=timedelta(seconds=30)
                )
            ],
        )
        result = await Runner.run(
            weather_agent,
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
        """Ask about the weather."""
        output = await workflow.execute_child_workflow(
            WeatherTurn.run,
            TurnInput(conversation=self._conversation, text=message.text),
            id=f"{workflow.info().workflow_id}-turn-{workflow.uuid4()}",
            task_queue=TURN_TASK_QUEUE,
        )
        self._conversation = output.conversation
        return TextReply(text=output.reply)
