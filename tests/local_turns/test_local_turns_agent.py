"""A harness agent whose turns run as local workflows: the turn's model and tool calls run in the
worker process, while the agent workflow, its message admission and its turn events stay on the
server.

Requires TEMPORAL_LOCAL_SERVER_MODULE: the path of a precompiled local-server module (.cwasm).
"""

import asyncio
import os
import uuid
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

with workflow.unsafe.imports_passed_through():
    from examples.local_turns_agent.workflow import (
        LocalTurnsAgentWorkflow,
        WeatherTurn,
        get_weather,
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
        SEND_AGENT_MESSAGE_UPDATE,
        TURN_EVENTS_TOPIC,
        AgentConfig,
        AgentEvent,
        AgentEventType,
        AgentMessage,
        AgentMessageReply,
    )
    from temporal_agent_harness.local_turns import LocalTurns


async def test_agent_turn_runs_locally():
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
    env = await WorkflowEnvironment.start_local(data_converter=pydantic_data_converter)
    try:
        async with LocalTurns(
            os.environ["TEMPORAL_LOCAL_SERVER_MODULE"],
            workflows=[WeatherTurn],
            activities=[get_weather],
            plugins=[plugin],
        ) as local_turns:
            task_queue = f"tq-{uuid.uuid4()}"
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[LocalTurnsAgentWorkflow],
                activities=[local_turns.run_turn],
            ):
                handle = await env.client.start_workflow(
                    LocalTurnsAgentWorkflow.run,
                    AgentConfig(),
                    id=f"agent-{uuid.uuid4()}",
                    task_queue=task_queue,
                )
                await handle.execute_update(
                    SEND_AGENT_MESSAGE_UPDATE,
                    AgentMessage(
                        type="ask",
                        payload={"text": "What is the weather in Boston?"},
                        expected_turn=1,
                    ),
                    result_type=AgentMessageReply,
                )
                events = await turn_events(env.client, handle.id)
                server_history = (await handle.fetch_history()).to_json_dict()
    finally:
        await env.shutdown()

    replies = [
        e.event.output
        for e in events
        if e.event.type == AgentEventType.MESSAGE_HANDLER_END
    ]
    assert replies == [{"text": "It is sunny in Boston."}]
    assert scheduled_activities(server_history) == ["run_local_turn"]


async def turn_events(client, workflow_id: str) -> list[AgentEvent]:
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
            if item.data.event.type == AgentEventType.TURN_END:
                return events
    return events


def scheduled_activities(history: dict[str, Any]) -> list[str]:
    return [
        e["activityTaskScheduledEventAttributes"]["activityType"]["name"]
        for e in history["events"]
        if "activityTaskScheduledEventAttributes" in e
    ]
