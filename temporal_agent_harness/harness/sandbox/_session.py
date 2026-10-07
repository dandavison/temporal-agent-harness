"""Temporal-aware sandbox session that routes all I/O through Temporal activities."""

from __future__ import annotations

import io
from datetime import timedelta
from pathlib import Path
from typing import Any, TypeVar

from agents.sandbox.session.base_sandbox_session import BaseSandboxSession
from agents.sandbox.session.pty_types import PtyExecUpdate
from agents.sandbox.session.sandbox_session_state import SandboxSessionState
from agents.sandbox.types import ExecResult, User
from pydantic import BaseModel

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError, TimeoutType
from temporal_agent_harness.harness.sandbox._activity_models import (
    ExecArgs,
    HydrateWorkspaceArgs,
    PersistWorkspaceArgs,
    PersistWorkspaceResult,
    PtyExecStartArgs,
    PtyExecUpdateResult,
    PtyWriteStdinArgs,
    ReadArgs,
    ReadResult,
    RunningArgs,
    RunningResult,
    StartArgs,
    StateResult,
    StopArgs,
    WriteArgs,
)
from temporal_agent_harness.harness.sandbox._activity_models import (
    ExecResult as ExecResultModel,
)
from temporalio.workflow import ActivityConfig

_ResultT = TypeVar("_ResultT", bound=BaseModel)

# How long an activity pinned to the session's worker may wait to be picked up before the
# worker is presumed gone. Generous, because a false positive is not free: it unpins the
# session, and any PTY process on that worker becomes unreachable.
DEFAULT_AFFINITY_TIMEOUT = timedelta(minutes=1)


def _is_schedule_to_start_timeout(error: ActivityError) -> bool:
    cause = error.cause
    return isinstance(cause, TimeoutError) and cause.type == TimeoutType.SCHEDULE_TO_START


class TemporalSandboxSession(BaseSandboxSession):
    """A BaseSandboxSession that routes all I/O through Temporal activities.

    This class is fully stateless with respect to the physical sandbox -- it
    holds only the serializable ``SandboxSessionState`` and a ``supports_pty``
    flag (both provided by the worker-side ``SessionResult``).

    Activity names are prefixed with the provider ``name`` so that dispatches
    reach the correct sandbox backend's activities on the worker.

    When the serving worker's provider has its own task queue (``task_queue``), every
    activity is routed there, because the live session -- and any PTY process it started --
    exists only in that worker's memory. If nothing picks a pinned activity up within
    ``affinity_timeout``, the worker is presumed gone: the session unpins (later calls go to
    the shared queue, where any worker resumes the session from its state) and the call
    fails with a non-retryable ``SandboxWorkerLost``.
    """

    def __init__(
        self,
        name: str,
        config: ActivityConfig,
        state: SandboxSessionState,
        supports_pty_flag: bool = True,
        task_queue: str | None = None,
        affinity_timeout: timedelta = DEFAULT_AFFINITY_TIMEOUT,
    ) -> None:
        """Initialize the session."""
        self._name = name
        self._config = config
        self._state = state
        self._supports_pty = supports_pty_flag
        self.task_queue = task_queue
        self.affinity_timeout = affinity_timeout

    @property
    def provider_name(self) -> str:
        """The name of the worker's ``SandboxClientProvider`` this session belongs to."""
        return self._name

    @property
    def state(self) -> SandboxSessionState:
        """The current session state."""
        return self._state

    @state.setter
    def state(self, value: SandboxSessionState) -> None:  # type: ignore[reportIncompatibleVariableOverride]
        self._state = value

    def pinned_activity_config(self, config: ActivityConfig) -> ActivityConfig:
        """``config`` routed to this session's worker, if it is pinned to one."""
        if self.task_queue is None:
            return config
        return {
            **config,
            "task_queue": self.task_queue,
            "schedule_to_start_timeout": self.affinity_timeout,
        }

    def unpin_if_worker_lost(self, error: ActivityError) -> bool:
        """Unpin when ``error`` says the pinned worker never picked the activity up.

        Returns whether it did, so a caller can turn the failure into ``SandboxWorkerLost``.
        """
        if self.task_queue is None or not _is_schedule_to_start_timeout(error):
            return False
        self.task_queue = None
        return True

    def worker_lost_error(self) -> ApplicationError:
        """The error for a call whose pinned worker never picked it up."""
        return sandbox_worker_lost()

    async def _call(
        self, op: str, arg: BaseModel, result_type: type[_ResultT] | None = None
    ) -> Any:
        try:
            return await workflow.execute_activity(
                f"{self._name}-{op}",
                arg=arg,
                result_type=result_type,
                **self.pinned_activity_config(self._config),
            )
        except ActivityError as e:
            if self.unpin_if_worker_lost(e):
                raise sandbox_worker_lost() from e
            raise

    async def exec(
        self,
        *command: str | Path,
        timeout: float | None = None,
        shell: bool | list[str] = True,
        user: str | User | None = None,
    ) -> ExecResult:
        """Execute a command in the sandbox via activity."""
        result: ExecResultModel = await self._call(
            "sandbox_session_exec",
            ExecArgs(
                state=self.state,
                command=[str(c) for c in command],
                timeout=timeout,
                shell=shell,
                user=user,
            ),
            ExecResultModel,
        )
        return ExecResult(
            stdout=result.stdout, stderr=result.stderr, exit_code=result.exit_code
        )

    async def _exec_internal(
        self,
        *command: str | Path,
        timeout: float | None = None,
    ) -> ExecResult:
        raise NotImplementedError("TemporalSandboxSession overrides exec() directly")

    async def read(self, path: Path, *, user: str | User | None = None) -> io.IOBase:
        """Read a file from the sandbox via activity."""
        result: ReadResult = await self._call(
            "sandbox_session_read", ReadArgs(state=self.state, path=str(path)), ReadResult
        )
        return io.BytesIO(result.data)

    async def write(
        self, path: Path, data: io.IOBase, *, user: str | User | None = None
    ) -> None:
        """Write a file to the sandbox via activity."""
        await self._call(
            "sandbox_session_write",
            WriteArgs(state=self.state, path=str(path), data=data.read()),
        )

    async def running(self) -> bool:
        """Check if the sandbox is running via activity."""
        result: RunningResult = await self._call(
            "sandbox_session_running", RunningArgs(state=self.state), RunningResult
        )
        return result.is_running

    async def shutdown(self) -> None:
        """Shut down the sandbox via activity."""
        self._update_state(
            await self._call(
                "sandbox_session_shutdown", StopArgs(state=self.state), StateResult
            )
        )

    async def persist_workspace(self) -> io.IOBase:
        """Persist the workspace via activity."""
        result: PersistWorkspaceResult = await self._call(
            "sandbox_session_persist_workspace",
            PersistWorkspaceArgs(state=self.state),
            PersistWorkspaceResult,
        )
        return io.BytesIO(result.data)

    async def hydrate_workspace(self, data: io.IOBase) -> None:
        """Hydrate the workspace via activity."""
        await self._call(
            "sandbox_session_hydrate_workspace",
            HydrateWorkspaceArgs(state=self.state, data=data.read()),
        )

    def supports_pty(self) -> bool:
        """Whether this session supports PTY operations."""
        return self._supports_pty

    async def pty_exec_start(
        self,
        *command: str | Path,
        timeout: float | None = None,
        shell: bool | list[str] = True,
        user: str | User | None = None,
        tty: bool = False,
        yield_time_s: float | None = None,
        max_output_tokens: int | None = None,
    ) -> PtyExecUpdate:
        """Start a PTY exec via activity."""
        result: PtyExecUpdateResult = await self._call(
            "sandbox_session_pty_exec_start",
            PtyExecStartArgs(
                state=self.state,
                command=[str(c) for c in command],
                timeout=timeout,
                shell=shell,
                user=user,
                tty=tty,
                yield_time_s=yield_time_s,
                max_output_tokens=max_output_tokens,
            ),
            PtyExecUpdateResult,
        )
        return PtyExecUpdate(
            process_id=result.process_id,
            output=result.output,
            exit_code=result.exit_code,
            original_token_count=result.original_token_count,
        )

    async def pty_write_stdin(
        self,
        *,
        session_id: int,
        chars: str,
        yield_time_s: float | None = None,
        max_output_tokens: int | None = None,
    ) -> PtyExecUpdate:
        """Write to PTY stdin via activity."""
        result: PtyExecUpdateResult = await self._call(
            "sandbox_session_pty_write_stdin",
            PtyWriteStdinArgs(
                state=self.state,
                session_id=session_id,
                chars=chars,
                yield_time_s=yield_time_s,
                max_output_tokens=max_output_tokens,
            ),
            PtyExecUpdateResult,
        )
        return PtyExecUpdate(
            process_id=result.process_id,
            output=result.output,
            exit_code=result.exit_code,
            original_token_count=result.original_token_count,
        )

    async def start(self) -> None:
        """Start the sandbox session via activity."""
        self._update_state(
            await self._call("sandbox_session_start", StartArgs(state=self.state), StateResult)
        )

    async def stop(self) -> None:
        """Stop the sandbox session via activity."""
        self._update_state(
            await self._call("sandbox_session_stop", StopArgs(state=self.state), StateResult)
        )

    def _update_state(self, result: StateResult | None) -> None:
        # ``None`` is a result recorded before these activities returned the state.
        if result is not None:
            self.state = result.state


def sandbox_worker_lost() -> ApplicationError:
    """The error a call fails with when the worker holding its session has gone away."""
    return ApplicationError(
        "the worker holding this sandbox session stopped responding. The sandbox itself is "
        "resumed from its saved state on the next call, but processes started earlier (for "
        "example an exec_command session) are gone; start them again.",
        type="SandboxWorkerLost",
        non_retryable=True,
    )
