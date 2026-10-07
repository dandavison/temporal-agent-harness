"""The workflow-owned lifecycle of an agent's sandbox."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, Literal

from agents.sandbox.session.sandbox_session_state import SandboxSessionState
from temporalio import workflow
from temporalio.exceptions import ApplicationError

from temporal_agent_harness.harness.sandbox._activity_models import DeleteSnapshotArgs
from temporal_agent_harness.harness.sandbox._client import TemporalSandboxClient
from temporal_agent_harness.harness.sandbox._session import TemporalSandboxSession
from temporal_agent_harness.harness.sandbox.config import SandboxConfig

# ``absent``: nothing created yet. ``idle``: a sandbox exists but is not running (shut down
# by the idle policy, or created and not yet started). ``running``: started and usable.
# ``closed``: torn down for good.
SandboxPhase = Literal["absent", "idle", "running", "closed"]


class SandboxLifecycle:
    """Creates, idles, resumes and closes one sandbox, from workflow code.

    Only this class changes the sandbox's lifecycle, and only through activities. What it
    keeps is the ``SandboxSessionState`` (inside its :class:`TemporalSandboxSession`) and a
    phase, both rebuilt on replay from activity results.
    """

    def __init__(self, config: SandboxConfig) -> None:
        self.config = config
        self.client = TemporalSandboxClient(
            config.client, config.activity_config, affinity_timeout=config.affinity_timeout
        )
        self._session: TemporalSandboxSession | None = None
        self._phase: SandboxPhase = "absent"
        # Parallel tool calls in one turn all ask for the sandbox; only one may create it.
        self._lock = asyncio.Lock()
        # Whether the sandbox has been used since the idle policy last ran.
        self._used_since_idle = False
        # Every snapshot this sandbox has written to, by id, for retention on close.
        self._snapshots: dict[str, SandboxSessionState] = {}

    @property
    def phase(self) -> SandboxPhase:
        return self._phase

    @property
    def idle_timer_armed(self) -> bool:
        """Whether an idle period should be timed: there is a policy with something to do,
        and the sandbox is running and has been used since the policy last ran."""
        idle = self.config.idle
        return (
            idle is not None
            and idle.action != "keep"
            and self._phase == "running"
            and self._used_since_idle
        )

    async def ensure_running(self) -> TemporalSandboxSession:
        """The running session, creating or resuming the sandbox first if needed."""
        async with self._lock:
            if self._phase == "closed":
                raise ApplicationError(
                    "this agent's sandbox has already been closed",
                    type="SandboxClosed",
                    non_retryable=True,
                )
            if self._phase == "absent":
                self._session = await self.client.create_session(
                    snapshot=self.config.snapshot,
                    manifest=self.config.manifest,
                    options=self.config.options,
                )
                self._phase = "idle"
                self._remember_snapshot()
                await self._session.start()
            elif self._phase == "idle":
                assert self._session is not None
                self._session = await self.client.resume_session(self._session.state)
                await self._session.start()
            self._phase = "running"
            self._used_since_idle = True
            self._remember_snapshot()
            assert self._session is not None
            return self._session

    async def apply_idle_policy(self) -> None:
        """Apply the idle action once, after the agent has been idle for ``idle.after``."""
        idle = self.config.idle
        async with self._lock:
            if idle is None or self._phase != "running":
                return
            session = self._session
            assert session is not None
            self._used_since_idle = False
            if idle.action in ("persist", "persist_and_shutdown"):
                await session.stop()
                self._remember_snapshot()
            if idle.action in ("persist_and_shutdown", "pause"):
                await session.shutdown()
                self._phase = "idle"

    async def close(self) -> None:
        """Tear the sandbox down: final persist (if the snapshot is kept), shut down, delete,
        then delete its snapshots (unless kept).

        A failed step does not skip the later ones. Failures are logged rather than raised,
        so a backend that refuses to clean up never fails the agent's own completion.
        """
        async with self._lock:
            phase, self._phase = self._phase, "closed"
            session = self._session
            if phase in ("absent", "closed") or session is None:
                return
            first_error: BaseException | None = None

            async def step(name: str, op: Callable[[], Awaitable[Any]]) -> None:
                nonlocal first_error
                try:
                    await op()
                except Exception as e:
                    workflow.logger.warning("sandbox close: %s failed: %s", name, e)
                    first_error = first_error or e

            if phase == "running":
                if self.config.keep_snapshot:
                    await step("stop", session.stop)
                    self._remember_snapshot()
                await step("shutdown", session.shutdown)
            await step("delete", lambda: self.client.delete(session))
            if not self.config.keep_snapshot:
                for state in list(self._snapshots.values()):
                    await step(
                        "snapshot delete",
                        lambda state=state: workflow.execute_activity(
                            f"{self.config.client}-sandbox_snapshot_delete",
                            arg=DeleteSnapshotArgs(state=state),
                            **self.config.activity_config,
                        ),
                    )
            if first_error is not None:
                workflow.logger.warning(
                    "sandbox close finished with errors; first: %s", first_error
                )

    def _remember_snapshot(self) -> None:
        if self._session is not None:
            state = self._session.state
            self._snapshots[state.snapshot.id] = state
