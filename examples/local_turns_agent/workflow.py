"""A weather agent whose turns run as local child workflows.

The agent workflow runs on the server and keeps the conversation; the harness runner admits
messages and publishes turn events as usual. Each turn is a child workflow, ``WeatherTurn``, that
the server owns and a worker process running ``LocalTurns`` runs: the OpenAI Agents SDK loop, with
its model and tool calls as activities of the turn. The server shows the turn's history as the
worker syncs it.

Compared with running the loop in the agent workflow, a turn loses the harness's tool approvals,
callback tools, tool events and streamed reply deltas: those go through the agent workflow, which
the turn cannot reach.
"""

from __future__ import annotations

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
    from temporal_agent_harness.local_turns import run_local_turn

MODEL = "gpt-5.6-luna"


@activity.defn
async def get_weather(city: str) -> str:
    """Get the weather for a city."""
    return f"sunny in {city}"


class TurnInput(BaseModel):
    conversation: list[dict[str, Any]]
    text: str


class TurnOutput(BaseModel):
    conversation: list[dict[str, Any]]
    reply: str


@workflow.defn
class WeatherTurn:
    """One turn, run as a local child workflow."""

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
        output = await run_local_turn(
            "WeatherTurn",
            TurnInput(conversation=self._conversation, text=message.text),
            result_type=TurnOutput,
        )
        self._conversation = output.conversation
        return TextReply(text=output.reply)
