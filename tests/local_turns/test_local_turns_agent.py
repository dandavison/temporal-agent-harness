"""A harness agent whose turns are child workflows on a task queue whose worker has local execution:
the turn's model and tool calls run in the worker process, the server shows the turn's progress while it runs, and the agent workflow, its
message admission and its turn events stay on the server.

Requirements: see tests/local_turns/server.py.
"""

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, TypeVar

from temporalio import activity, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.worker import LocalExecution, Worker

with workflow.unsafe.imports_passed_through():
    from examples.local_turns_agent.workflow import (
        TURN_TASK_QUEUE,
        LocalTurnsAgentWorkflow,
        WeatherTurn,
    )

    from temporal_agent_harness.ai_sdks.openai_agents import (
        ModelActivityParameters,
        OpenAIAgentsPlugin,
    )
    from temporal_agent_harness.ai_sdks.openai_agents.testing import (
        ResponseBuilders,
        TestModel,
        TestModelProvider,
    )
    from temporal_agent_harness.harness.agent_protocol import (
        AGENT_STATUS_QUERY,
        SEND_AGENT_MESSAGE_UPDATE,
        TURN_EVENTS_TOPIC,
        AgentConfig,
        AgentEvent,
        AgentEventType,
        AgentMessage,
        AgentMessageReply,
        AgentStatus,
    )
    from tests.local_turns.server import start_local_execution_server

T = TypeVar("T")

weather_may_return = asyncio.Event()


@activity.defn(name="get_weather")
async def gated_get_weather(city: str) -> str:
    await weather_may_return.wait()
    return f"sunny in {city}"


async def test_agent_turn_runs_as_a_local_child_workflow():
    weather_may_return.clear()
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
                activities=[gated_get_weather],
                plugins=[plugin],
                local_execution=LocalExecution(),
            ),
            Worker(
                env.client, task_queue=task_queue, workflows=[LocalTurnsAgentWorkflow]
            ),
        ):
            agent = await env.client.start_workflow(
                LocalTurnsAgentWorkflow.run,
                AgentConfig(),
                id=f"agent-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            reply = asyncio.create_task(
                agent.execute_update(
                    SEND_AGENT_MESSAGE_UPDATE,
                    AgentMessage(
                        type="ask",
                        payload={"text": "What is the weather in Boston?"},
                        expected_turn=1,
                    ),
                    result_type=AgentMessageReply,
                )
            )

            # While the turn waits in its tool call, the server shows its model call...
            turn = await eventually(lambda: turn_handle(env.client, agent))
            await eventually(lambda: activity_completed(turn))
            # ...and the agent workflow serves a query and the updates that read its events.
            status = await agent.query(AGENT_STATUS_QUERY, result_type=AgentStatus)
            assert status.turn_active
            events = await turn_events(
                env.client, agent.id, AgentEventType.TURN_STARTED
            )
            assert events[-1].event.type == AgentEventType.TURN_STARTED

            weather_may_return.set()
            await reply
            events = await turn_events(env.client, agent.id, AgentEventType.TURN_END)
            agent_history = (await agent.fetch_history()).to_json_dict()
            turn_history = (await turn.fetch_history()).to_json_dict()
    finally:
        await env.shutdown()

    replies = [
        e.event.output
        for e in events
        if e.event.type == AgentEventType.MESSAGE_HANDLER_END
    ]
    assert replies == [{"text": "It is sunny in Boston."}]
    assert scheduled_activities(agent_history) == []
    assert scheduled_activities(turn_history) == [
        "invoke_model_activity",
        "get_weather",
        "invoke_model_activity",
    ]


async def eventually(get: Callable[[], Awaitable[T | None]]) -> T:
    async with asyncio.timeout(30):
        while (value := await get()) is None:
            await asyncio.sleep(0.1)
    return value


async def turn_handle(client: Client, agent: WorkflowHandle) -> WorkflowHandle | None:
    async for event in agent.fetch_history_events():
        if event.HasField("child_workflow_execution_started_event_attributes"):
            attrs = event.child_workflow_execution_started_event_attributes
            return client.get_workflow_handle(attrs.workflow_execution.workflow_id)
    return None


async def activity_completed(handle: WorkflowHandle) -> bool | None:
    async for event in handle.fetch_history_events():
        if event.HasField("activity_task_completed_event_attributes"):
            return True
    return None


async def turn_events(
    client: Client, workflow_id: str, until: AgentEventType
) -> list[AgentEvent]:
    stream = WorkflowStreamClient.create(client, workflow_id)
    events: list[AgentEvent] = []
    async with asyncio.timeout(60):
        async for item in stream.subscribe(
            topics=[TURN_EVENTS_TOPIC],
            from_offset=0,
            result_type=AgentEvent,
            poll_cooldown=timedelta(milliseconds=10),
        ):
            events.append(item.data)
            if item.data.event.type == until:
                return events
    return events


def scheduled_activities(history: dict[str, Any]) -> list[str]:
    return [
        e["activityTaskScheduledEventAttributes"]["activityType"]["name"]
        for e in history["events"]
        if "activityTaskScheduledEventAttributes" in e
    ]
