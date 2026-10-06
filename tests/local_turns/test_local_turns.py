"""A server-hosted agent workflow runs a turn as one activity, which runs the turn's model and tool
calls as a local workflow in the worker process.

Requires TEMPORAL_LOCAL_SERVER_MODULE: the path of a precompiled local-server module (.cwasm).
"""

import os
import uuid
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

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
    from temporal_agent_harness.local_turns import (
        LocalTurn,
        LocalTurnResult,
        LocalTurns,
    )


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
    """The server-hosted agent: each turn is one activity."""

    @workflow.run
    async def run(self, prompt: str) -> LocalTurnResult:
        return await workflow.execute_activity(
            LocalTurns.run_turn,
            LocalTurn(workflow="WeatherTurn", input=prompt),
            start_to_close_timeout=timedelta(seconds=60),
        )


async def test_turn_runs_as_one_server_activity_and_several_local_activities():
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
    env = await WorkflowEnvironment.start_local()
    try:
        async with LocalTurns(
            os.environ["TEMPORAL_LOCAL_SERVER_MODULE"],
            workflows=[WeatherTurn],
            activities=[get_weather],
            plugins=[plugin],
        ) as local_turns:
            server_client = env.client
            task_queue = f"tq-{uuid.uuid4()}"
            async with Worker(
                server_client,
                task_queue=task_queue,
                workflows=[WeatherAgent],
                activities=[local_turns.run_turn],
            ):
                handle = await server_client.start_workflow(
                    WeatherAgent.run,
                    "What is the weather in Boston?",
                    id=f"wf-{uuid.uuid4()}",
                    task_queue=task_queue,
                )
                result = await handle.result()
            server_history = (await handle.fetch_history()).to_json_dict()
    finally:
        await env.shutdown()

    assert result.output == "It is sunny in Boston."
    assert scheduled_activities(server_history) == ["run_local_turn"]
    assert scheduled_activities(result.history) == [
        "invoke_model_activity",
        "get_weather",
        "invoke_model_activity",
    ]


def scheduled_activities(history: dict[str, Any]) -> list[str]:
    return [
        e["activityTaskScheduledEventAttributes"]["activityType"]["name"]
        for e in history["events"]
        if "activityTaskScheduledEventAttributes" in e
    ]
