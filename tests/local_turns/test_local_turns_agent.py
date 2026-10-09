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
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.worker import LocalExecution, Worker

with workflow.unsafe.imports_passed_through():
    from examples.local_turns_agent.workflow import (
        TURN_ENDPOINT,
        TURN_HANDLER_TASK_QUEUE,
        TURN_TASK_QUEUE,
        FilesTurn,
        LocalTurnsAgentWorkflow,
        LocalTurnsNexusAgentWorkflow,
        TurnService,
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

file_may_return = asyncio.Event()


@activity.defn(name="read_file")
async def gated_read_file(file: str) -> str:
    await file_may_return.wait()
    return f"the contents of {file}"


async def test_agent_turn_runs_as_a_local_child_workflow():
    file_may_return.clear()
    model = TestModel.returning_responses(
        [
            ResponseBuilders.tool_call('{"file":"README.md"}', "read_file"),
            ResponseBuilders.output_message("It is a README."),
        ]
    )
    plugin = OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(
            start_to_close_timeout=timedelta(seconds=30)
        ),
        model_provider=TestModelProvider(model),
    )
    env = await start_local_execution_server()
    try:
        # The plugin sets the client's data converter for the model activities' arguments.
        client = Client(**{**env.client.config(), "plugins": [plugin]})
        task_queue = f"tq-{uuid.uuid4()}"
        async with (
            Worker(
                client,
                task_queue=TURN_TASK_QUEUE,
                workflows=[FilesTurn],
                activities=[gated_read_file],
                local_execution=LocalExecution(),
            ),
            Worker(client, task_queue=task_queue, workflows=[LocalTurnsAgentWorkflow]),
        ):
            agent = await client.start_workflow(
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
                        payload={"text": "What is in README.md?"},
                        expected_turn=1,
                    ),
                    result_type=AgentMessageReply,
                )
            )

            # While the turn waits in its tool call, the server shows its model call...
            turn = await eventually(lambda: turn_handle(client, agent))
            await eventually(lambda: activity_completed(turn))
            # ...and the agent workflow serves a query and the updates that read its events.
            status = await agent.query(AGENT_STATUS_QUERY, result_type=AgentStatus)
            assert status.turn_active
            events = await turn_events(client, agent.id, AgentEventType.TURN_STARTED)
            assert events[-1].event.type == AgentEventType.TURN_STARTED

            file_may_return.set()
            await reply
            events = await turn_events(client, agent.id, AgentEventType.TURN_END)
            agent_history = (await agent.fetch_history()).to_json_dict()
            turn_history = (await turn.fetch_history()).to_json_dict()
    finally:
        await env.shutdown()

    replies = [
        e.event.output
        for e in events
        if e.event.type == AgentEventType.MESSAGE_HANDLER_END
    ]
    assert replies == [{"text": "It is a README."}]
    assert scheduled_activities(agent_history) == []
    assert scheduled_activities(turn_history) == [
        "invoke_model_activity",
        "read_file",
        "invoke_model_activity",
    ]


async def test_agent_turn_runs_as_a_local_workflow_behind_a_nexus_operation():
    file_may_return.set()
    model = TestModel.returning_responses(
        [
            ResponseBuilders.tool_call('{"file":"README.md"}', "read_file"),
            ResponseBuilders.output_message("It is a README."),
        ]
    )
    plugin = OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(
            start_to_close_timeout=timedelta(seconds=30)
        ),
        model_provider=TestModelProvider(model),
    )
    env = await start_local_execution_server()
    try:
        client = Client(**{**env.client.config(), "plugins": [plugin]})
        await env.create_nexus_endpoint(TURN_ENDPOINT, TURN_HANDLER_TASK_QUEUE)
        task_queue = f"tq-{uuid.uuid4()}"
        async with (
            # The only worker for the turn's task queue has local execution, so the turn runs
            # locally; its result reaches the agent through the operation's completion callback.
            Worker(
                client,
                task_queue=TURN_TASK_QUEUE,
                workflows=[FilesTurn],
                activities=[gated_read_file],
                local_execution=LocalExecution(),
            ),
            Worker(
                client,
                task_queue=TURN_HANDLER_TASK_QUEUE,
                nexus_service_handlers=[TurnService()],
            ),
            Worker(
                client, task_queue=task_queue, workflows=[LocalTurnsNexusAgentWorkflow]
            ),
        ):
            agent = await client.start_workflow(
                LocalTurnsNexusAgentWorkflow.run,
                AgentConfig(),
                id=f"agent-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            await agent.execute_update(
                SEND_AGENT_MESSAGE_UPDATE,
                AgentMessage(
                    type="ask",
                    payload={"text": "What is in README.md?"},
                    expected_turn=1,
                ),
                result_type=AgentMessageReply,
            )
            events = await turn_events(client, agent.id, AgentEventType.TURN_END)
            agent_history = (await agent.fetch_history()).to_json_dict()
    finally:
        await env.shutdown()

    replies = [
        e.event.output
        for e in events
        if e.event.type == AgentEventType.MESSAGE_HANDLER_END
    ]
    assert replies == [{"text": "It is a README."}]
    assert scheduled_activities(agent_history) == []
    assert any(
        "nexusOperationCompletedEventAttributes" in e for e in agent_history["events"]
    )


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
