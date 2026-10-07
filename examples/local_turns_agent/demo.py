"""Chat with an agent whose turns run as local child workflows, and watch them in the Temporal UI.

The agent workflow runs on the server. Each turn runs in this process, against an in-process local
server that syncs the turn's history to the server. See README.md.

    uv run --group examples python -m examples.local_turns_agent.demo
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from datetime import timedelta

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.worker import Worker

from temporal_agent_harness.ai_sdks.openai_agents import (
    ModelActivityParameters,
    OpenAIAgentsPlugin,
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

from .workflow import LocalTurnsAgentWorkflow, WeatherTurn, get_weather

TASK_QUEUE = "local-turns-agent"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="localhost:7233")
    parser.add_argument("--ui", default="http://localhost:8233")
    args = parser.parse_args()
    module = os.environ.get("TEMPORAL_LOCAL_SERVER_MODULE")
    if not module:
        sys.exit("error: TEMPORAL_LOCAL_SERVER_MODULE env var not set")
    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("error: OPENAI_API_KEY env var not set")

    client = await Client.connect(args.address, data_converter=pydantic_data_converter)
    plugin = OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(
            start_to_close_timeout=timedelta(seconds=60)
        )
    )
    async with (
        LocalTurns(
            module,
            args.address,
            workflows=[WeatherTurn],
            activities=[get_weather],
            plugins=[plugin],
        ),
        Worker(client, task_queue=TASK_QUEUE, workflows=[LocalTurnsAgentWorkflow]),
    ):
        agent = await client.start_workflow(
            LocalTurnsAgentWorkflow.run,
            AgentConfig(),
            id=f"local-turns-demo-{uuid.uuid4().hex[:8]}",
            task_queue=TASK_QUEUE,
        )
        print(f"Agent workflow: {args.ui}/namespaces/default/workflows/{agent.id}")
        print("Ask about the weather; an empty line quits.")
        stream = WorkflowStreamClient.create(client, agent.id)
        offset = 0
        turn = 1
        while text := await asyncio.to_thread(input, "> "):
            await agent.execute_update(
                SEND_AGENT_MESSAGE_UPDATE,
                AgentMessage(type="ask", payload={"text": text}, expected_turn=turn),
                result_type=AgentMessageReply,
            )
            async for item in stream.subscribe(
                topics=[TURN_EVENTS_TOPIC], from_offset=offset, result_type=AgentEvent
            ):
                offset = item.offset + 1
                event = item.data.event
                if event.type == AgentEventType.MESSAGE_HANDLER_END:
                    print(event.output["text"])
                if event.type == AgentEventType.TURN_END:
                    break
            turn += 1
        await agent.terminate("demo finished")


if __name__ == "__main__":
    asyncio.run(main())
