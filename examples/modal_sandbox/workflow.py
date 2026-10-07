"""A Gemini coding agent that works in one durable Modal sandbox.

The model gets the full sandbox tool suite from ``runner.sandbox_tools()``: ``exec_command`` and
``write_stdin`` (the Shell capability: commands on a PTY, including long-running processes it can
check back on, or type into, later) and ``view_image`` and ``apply_patch`` (the Filesystem
capability). Their descriptions, and the capabilities' own prompt fragments
(``runner.sandbox_instructions()``), come from the OpenAI sandbox layer.

The sandbox is created on the first tool call, persisted and shut down after ``IDLE_AFTER``
without a message, resumed with its workspace restored on the next one, and deleted when the
session closes. Running processes do not survive an idle shutdown; files do.

The turn loop is the Gemini Interactions loop from ``examples/callback_tools/wiki_agent``. See
``README.md`` for the run steps.
"""

import asyncio
from datetime import timedelta
from pathlib import Path

from temporalio import workflow
from temporalio.contrib.workflow_streams import WorkflowStream
from temporalio.exceptions import ActivityError
from temporalio.workflow import ActivityConfig

with workflow.unsafe.imports_passed_through():
    from agents.extensions.sandbox.modal import ModalSandboxClientOptions
    from agents.sandbox import LocalSnapshotSpec
    from agents.sandbox.capabilities import Filesystem, Shell
    from google.genai._interactions.types import FunctionCallStep
    from google.genai._interactions.types.function_result_step_param import (
        FunctionResultStepParam,
    )

    from temporal_agent_harness.ai_sdks.google_genai_plugin import (
        InteractionConversation,
        function_param,
        function_result_value,
        google_genai_client,
    )
    from temporal_agent_harness.harness import AgentWorkflowRunner, agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        TextMessage,
        TextReply,
        ToolApprovalPolicy,
    )
    from temporal_agent_harness.harness.sandbox import IdlePolicy, SandboxConfig

TASK_QUEUE = "modal-sandbox"
# The name the worker registers its SandboxClientProvider under.
PROVIDER = "modal"
DEFAULT_MODEL = "gemini-3.5-flash"

# How long the agent sits idle before its sandbox is persisted and shut down. Long enough that a
# server started in one turn is still running when the user comes back to ask about it.
IDLE_AFTER = timedelta(minutes=10)
# Modal's hard lifetime for one sandbox, counted from creation. Modal terminates the sandbox when it
# runs out, whatever the agent is doing.
MODAL_SANDBOX_TIMEOUT = timedelta(hours=1)
# Snapshots live on the worker's disk, so this needs a single worker (hence ``dev_mode``).
SNAPSHOT_DIR = Path("/tmp/harness-modal-sandbox-snapshots")

SANDBOX = SandboxConfig(
    client=PROVIDER,
    options=ModalSandboxClientOptions(
        app_name="temporal-agent-harness-sandbox",
        timeout=int(MODAL_SANDBOX_TIMEOUT.total_seconds()),
    ),
    snapshot=LocalSnapshotSpec(base_path=SNAPSHOT_DIR),
    dev_mode=True,
    capabilities=[Shell(), Filesystem()],
    idle=IdlePolicy(after=IDLE_AFTER, action="persist_and_shutdown"),
)

SYSTEM_INSTRUCTION = f"""\
You are a capable software engineer working in a Linux sandbox (Debian, Python 3.14, no other
language runtimes preinstalled; you can install packages with pip or apt-get). Do the user's task
in the sandbox with your tools, then tell them briefly what you did and what you found.

- Run commands with `exec_command`. If a command is still running when it returns (a server, a
  watcher, a long build), you get a session id: check back on it with `write_stdin` and empty
  `chars` to read new output, or type into it (pass `tty: true` to `exec_command` for an
  interactive program). Its output is only what arrived since your last check.
- A process you started in an earlier turn may still be running; check its session before
  starting another.
- After {int(IDLE_AFTER.total_seconds() // 60)} minutes with no message from the user the
  sandbox is saved and shut down. Files in the workspace survive that; running processes do
  not, so if a session id no longer works, start the process again.
- Edit files with `apply_patch`. Look at images (plots, screenshots) with `view_image`.
"""


@agent.defn(name="ModalCodingAgent")
class ModalCodingAgentWorkflow:
    """A Gemini coding agent with a durable Modal sandbox and the full sandbox tool suite."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            # Everything the model does happens inside its own sandbox.
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
            sandbox=SANDBOX,
        )
        self._conversation = InteractionConversation()
        self._tools = self._runner.sandbox_tools()
        self._tools_by_name = {tool.__name__: tool for tool in self._tools}
        self._system_instruction = (
            f"{SYSTEM_INSTRUCTION}\n{self._runner.sandbox_instructions()}"
        )
        self._gemini = google_genai_client(
            activity_config=ActivityConfig(start_to_close_timeout=timedelta(minutes=3)),
            runner=self._runner,
        )

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Give the agent a task. It works in its own Linux sandbox: it can run commands,
        leave servers or long jobs running and check back on them, edit files, and look at
        images it produces."""
        tools = [function_param(tool) for tool in self._tools]
        self._conversation.add_user_text(message.text)
        while True:
            stream = await self._gemini.interactions.create(
                model=DEFAULT_MODEL,
                input=self._conversation.steps,
                system_instruction=self._system_instruction,
                tools=tools,
                stream=True,
            )
            reply = await self._conversation.read_reply(stream)
            if not reply.function_calls:
                return TextReply(text=reply.text)
            self._conversation.add_function_results(
                await asyncio.gather(*(self._run_tool(call) for call in reply.function_calls))
            )

    async def _run_tool(self, call: FunctionCallStep) -> FunctionResultStepParam:
        """Run one tool call; a failure goes back to the model as an error result."""
        response: FunctionResultStepParam = {
            "type": "function_result",
            "call_id": call.id,
            "name": call.name,
            "result": "",
        }
        if call.signature:
            response["signature"] = call.signature
        try:
            tool = self._tools_by_name.get(call.name)
            if tool is None:
                raise ValueError(f"unknown tool: {call.name!r}")
            result = await self._runner.run_tool(call.id, tool, **call.arguments)
            response["result"] = function_result_value(result)
        except Exception as e:
            response["result"] = _error_text(e)
            response["is_error"] = True
        return response


def _error_text(error: BaseException) -> str:
    """What a failed tool call tells the model. A failed activity tool raises Temporal's
    ``ActivityError`` ("Activity task failed"); the reason is its cause."""
    while isinstance(error, ActivityError) and error.cause is not None:
        error = error.cause
    return str(error)
