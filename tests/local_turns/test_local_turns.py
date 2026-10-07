"""A server-hosted agent workflow runs a turn as a child workflow on a task queue whose worker has
local execution: the worker's local server acquires the child from the server, runs the turn's model
and tool calls in-process, and syncs the child's history to the server.

Requirements: see tests/local_turns/server.py.
"""

import uuid
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.client import WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import LocalExecution, Worker

with workflow.unsafe.imports_passed_through():
    from agents import Agent, Runner

    from temporal_agent_harness.ai_sdks.openai_agents import (
        ModelActivityParameters,
        OpenAIAgentsPlugin,
    )
    from temporal_agent_harness.ai_sdks.openai_agents.testing import (
        ResponseBuilders,
        TestModel,
        TestModelProvider,
    )
    from temporal_agent_harness.ai_sdks.openai_agents.workflow import activity_as_tool
    from tests.local_turns.server import start_local_execution_server

TURN_TASK_QUEUE = "turns"


@activity.defn
async def get_weather(city: str) -> str:
    return f"sunny in {city}"


@workflow.defn
class WeatherTurn:
    """One turn of a weather agent: its model and tool calls are activities."""

    @workflow.run
    async def run(self, prompt: str) -> str:
        agent = Agent(
            name="weather",
            instructions="Answer weather questions.",
            tools=[
                activity_as_tool(
                    get_weather, start_to_close_timeout=timedelta(seconds=10)
                )
            ],
        )
        return str((await Runner.run(agent, input=prompt)).final_output)


@workflow.defn
class WeatherAgent:
    """The server-hosted agent: each turn is a child workflow."""

    @workflow.run
    async def run(self, prompt: str) -> str:
        return await workflow.execute_child_workflow(
            WeatherTurn.run, prompt, task_queue=TURN_TASK_QUEUE
        )


async def test_turn_runs_as_a_local_child_workflow_synced_to_the_server():
    model = TestModel.returning_responses(
        [
            ResponseBuilders.tool_call('{"city":"Boston"}', "get_weather"),
            ResponseBuilders.output_message("It is sunny in Boston."),
        ]
    )
    plugin = OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(
            start_to_close_timeout=timedelta(seconds=30)
        ),
        model_provider=TestModelProvider(model),
    )
    env = await start_local_execution_server(data_converter=pydantic_data_converter)
    try:
        task_queue = f"tq-{uuid.uuid4()}"
        async with (
            Worker(
                env.client,
                task_queue=TURN_TASK_QUEUE,
                workflows=[WeatherTurn],
                activities=[get_weather],
                plugins=[plugin],
                local_execution=LocalExecution(),
            ),
            Worker(env.client, task_queue=task_queue, workflows=[WeatherAgent]),
        ):
            handle = await env.client.start_workflow(
                WeatherAgent.run,
                "What is the weather in Boston?",
                id=f"wf-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            result = await handle.result()
        agent_history = await handle.fetch_history()
        turn_id = child_workflow_ids(agent_history)[0]
        turn_history = await env.client.get_workflow_handle(turn_id).fetch_history()
    finally:
        await env.shutdown()

    assert result == "It is sunny in Boston."
    assert scheduled_activities(agent_history.to_json_dict()) == []
    assert scheduled_activities(turn_history.to_json_dict()) == [
        "invoke_model_activity",
        "get_weather",
        "invoke_model_activity",
    ]


def child_workflow_ids(history: WorkflowHistory) -> list[str]:
    return [
        e.child_workflow_execution_started_event_attributes.workflow_execution.workflow_id
        for e in history.events
        if e.HasField("child_workflow_execution_started_event_attributes")
    ]


def scheduled_activities(history: dict[str, Any]) -> list[str]:
    return [
        e["activityTaskScheduledEventAttributes"]["activityType"]["name"]
        for e in history["events"]
        if "activityTaskScheduledEventAttributes" in e
    ]
