"""Agents and tools for tests/harness/test_sandbox.py, in their own module so the workflows
run under the real sandboxed workflow runner the way an agent author's would."""

from __future__ import annotations

import dataclasses
from datetime import timedelta
from pathlib import Path
from typing import Any

from temporalio import activity, workflow
from temporalio.exceptions import ActivityError
from temporalio.contrib.workflow_streams import WorkflowStream

with workflow.unsafe.imports_passed_through():
    from agents import RunConfig, Runner
    from agents.sandbox import LocalSnapshotSpec, SandboxAgent
    from agents.sandbox.capabilities import Filesystem, Shell
    from agents.sandbox.session.base_sandbox_session import BaseSandboxSession

    from temporal_agent_harness.harness import AgentWorkflowRunner, agent
    from temporal_agent_harness.harness.agent import Injected
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        TextMessage,
        TextReply,
        ToolApprovalPolicy,
    )
    from temporal_agent_harness.harness.sandbox import IdlePolicy, SandboxConfig, SandboxSession
    from temporal_agent_harness.harness.sandbox.tools import (
        apply_patch,
        exec_command,
        view_image,
    )

PROVIDER = "local"
# Fixed rather than per-import: the workflow sandbox re-imports this module per run.
SNAPSHOT_DIR = Path("/tmp/harness-sandbox-test-snapshots")
IDLE_AFTER = timedelta(minutes=5)

SANDBOX = SandboxConfig(
    client=PROVIDER,
    snapshot=LocalSnapshotSpec(base_path=SNAPSHOT_DIR),
    dev_mode=True,
    capabilities=[Shell(), Filesystem()],
    idle=IdlePolicy(after=IDLE_AFTER, action="persist_and_shutdown"),
)


@agent.activity_tool_defn()
async def run_in_sandbox(session: Injected[SandboxSession], command: str) -> str:
    """Run a shell command in the sandbox and return its stdout."""
    result = await session.exec(command)
    return result.stdout.decode()


@agent.activity_tool_defn()
async def serving_task_queue(session: Injected[SandboxSession]) -> str:
    """The task queue of the worker that ran this tool."""
    assert isinstance(session, SandboxSession)
    return activity.info().task_queue


@agent.tool_defn()
async def list_workspace(session: Injected[BaseSandboxSession]) -> str:
    """List the workspace from workflow code, through the activity-backed session."""
    result = await session.exec("ls")
    return result.stdout.decode()


async def _call(runner: AgentWorkflowRunner, tool: Any, **kwargs: Any) -> str:
    try:
        return str(await runner.run_tool(str(workflow.uuid4()), tool, **kwargs))
    except Exception as e:
        cause = e.cause if isinstance(e, ActivityError) and e.cause is not None else e
        return f"error: {cause} ({getattr(e.__cause__, 'type', None) or getattr(e, 'type', None)})"


@agent.defn
class SandboxProbeAgent:
    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
            sandbox=SANDBOX,
        )

    @agent.accepts
    async def write_files(self, message: TextMessage) -> TextReply:
        """Exercise every way a tool reaches the sandbox."""
        r = self._runner
        results = {
            "custom": await _call(
                r, run_in_sandbox, command=f"printf '{message.text}' > a.txt && cat a.txt"
            ),
            "exec_command": await _call(r, exec_command, cmd="cat a.txt"),
            "apply_patch": await _call(
                r,
                apply_patch,
                patch="*** Begin Patch\n*** Add File: b.txt\n+bee\n*** End Patch\n",
            ),
            "png": await _call(
                r, exec_command, cmd="printf '\\211PNG\\r\\n\\032\\n0000' > img.png"
            ),
            "view_image": await _call(r, view_image, path="img.png"),
            "inline": await _call(r, list_workspace),
        }
        return TextReply(text=repr(results))

    @agent.accepts
    async def read_file(self, message: TextMessage) -> TextReply:
        """Read a workspace file back through a custom tool."""
        return TextReply(text=await _call(self._runner, run_in_sandbox, command=f"cat {message.text}"))

    @agent.accepts
    async def bad_patches(self, message: TextMessage) -> TextReply:
        """Send apply_patch two malformed patches the model could plausibly write."""
        results = {
            "plus_end_marker": await _call(
                self._runner,
                apply_patch,
                patch="*** Begin Patch\n*** Add File: c.txt\n+sea\n+*** End Patch",
            ),
            "no_begin_marker": await _call(
                self._runner, apply_patch, patch="*** Add File: c.txt\n+sea\n*** End Patch"
            ),
        }
        return TextReply(text=repr(results))

    @agent.accepts
    async def where(self, message: TextMessage) -> TextReply:
        """Report which worker task queue served a sandbox tool."""
        return TextReply(text=await _call(self._runner, serving_task_queue))

    @agent.accepts
    async def instructions(self, message: TextMessage) -> TextReply:
        """The capability instructions and tool names the runner offers."""
        names = [t.__name__ for t in self._runner.sandbox_tools()]
        return TextReply(text=f"{names}\n{self._runner.sandbox_instructions()}")


@agent.defn
class NoSandboxAgent:
    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
        )

    @agent.accepts
    async def try_tool(self, message: TextMessage) -> TextReply:
        """Call a sandbox tool on an agent that has no sandbox."""
        return TextReply(text=await _call(self._runner, run_in_sandbox, command="true"))


@agent.defn
class PinnedProbeAgent:
    """An agent whose sandbox gives up on an unresponsive pinned worker after two seconds."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
            sandbox=dataclasses.replace(SANDBOX, affinity_timeout=timedelta(seconds=2)),
        )

    @agent.accepts
    async def where(self, message: TextMessage) -> TextReply:
        """Report which worker task queue served a sandbox tool."""
        return TextReply(text=await _call(self._runner, serving_task_queue))


@agent.defn
class OpenAISandboxAgent:
    """Runs an OpenAI SandboxAgent each turn, on the runner's own sandbox."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
            sandbox=SANDBOX,
        )

    @agent.accepts
    async def run_agent(self, message: TextMessage) -> TextReply:
        """One SandboxAgent run."""
        result = await Runner.run(
            SandboxAgent(name="coder", instructions="Use the shell.", capabilities=[Shell()]),
            message.text,
            run_config=RunConfig(sandbox=await self._runner.sandbox_run_config()),
        )
        return TextReply(text=str(result.final_output))

    @agent.accepts
    async def read_file(self, message: TextMessage) -> TextReply:
        """Read a workspace file back through a custom tool."""
        return TextReply(text=await _call(self._runner, run_in_sandbox, command=f"cat {message.text}"))
