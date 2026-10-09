"""Chat with an agent whose turns run as local child workflows, and watch them in the Temporal UI.

The agent workflow runs on the server. Each turn runs in this process, on a worker with local
execution, which syncs the turn's history to the server. See README.md.

    uv run --group examples python -m examples.local_turns_agent.demo [--nexus]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from datetime import timedelta
from pathlib import Path

from temporalio.api.nexus.v1 import EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import CreateNexusEndpointRequest
from temporalio.client import Client
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import LocalExecution, Worker

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

from .workflow import (
    TURN_ENDPOINT,
    TURN_HANDLER_TASK_QUEUE,
    TURN_TASK_QUEUE,
    FilesTurn,
    LocalTurnsAgentWorkflow,
    LocalTurnsNexusAgentWorkflow,
    TurnService,
    list_files,
    read_file,
)

TASK_QUEUE = "local-turns-agent"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="localhost:7233")
    parser.add_argument("--ui", default="http://localhost:8233")
    parser.add_argument(
        "--nexus",
        action="store_true",
        help="start each turn through a Nexus operation instead of as a child workflow",
    )
    args = parser.parse_args()
    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("error: OPENAI_API_KEY env var not set")

    plugin = OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(
            start_to_close_timeout=timedelta(seconds=60)
        )
    )
    client = await Client.connect(args.address, plugins=[plugin])
    agent_workflow = LocalTurnsAgentWorkflow
    if args.nexus:
        agent_workflow = LocalTurnsNexusAgentWorkflow
        await create_turn_endpoint(client)
    async with (
        Worker(
            client,
            task_queue=TURN_TASK_QUEUE,
            workflows=[FilesTurn],
            activities=[list_files, read_file],
            local_execution=LocalExecution(),
        ),
        Worker(
            client,
            task_queue=TURN_HANDLER_TASK_QUEUE,
            nexus_service_handlers=[TurnService()],
        ),
        Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[LocalTurnsAgentWorkflow, LocalTurnsNexusAgentWorkflow],
        ),
    ):
        agent = await client.start_workflow(
            agent_workflow.run,
            AgentConfig(),
            id=f"local-turns-demo-{uuid.uuid4().hex[:8]}",
            task_queue=TASK_QUEUE,
        )
        print(f"Agent workflow: {args.ui}/namespaces/default/workflows/{agent.id}")
        print(f"{Path.cwd()}")
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


async def create_turn_endpoint(client: Client) -> None:
    """Creates the Nexus endpoint for TurnService, unless the server already has it."""
    try:
        await client.operator_service.create_nexus_endpoint(
            CreateNexusEndpointRequest(
                spec=EndpointSpec(
                    name=TURN_ENDPOINT,
                    target=EndpointTarget(
                        worker=EndpointTarget.Worker(
                            namespace=client.namespace,
                            task_queue=TURN_HANDLER_TASK_QUEUE,
                        )
                    ),
                )
            )
        )
    except RPCError as err:
        if err.status != RPCStatusCode.ALREADY_EXISTS:
            raise


if __name__ == "__main__":
    asyncio.run(main())
