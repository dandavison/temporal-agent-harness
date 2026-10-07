"""Model-facing sandbox tools, built from the OpenAI sandbox capabilities.

Each is an ordinary activity tool with an ``Injected[SandboxSession]`` parameter, so any
model SDK (and Code Mode) can use it. Its name, description and parameters come from the
OpenAI tool it wraps, and its body runs that tool against the agent's real session.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from datetime import timedelta
from typing import Annotated, Any

from agents.sandbox import Manifest
from agents.sandbox.capabilities import Capability
from agents.sandbox.capabilities.tools import (
    ExecCommandTool,
    SandboxApplyPatchTool,
    ViewImageTool,
    WriteStdinTool,
)
from agents.sandbox.capabilities.tools.apply_patch_tool import (
    _APPLY_PATCH_CUSTOM_TOOL_DESCRIPTION,
)
from agents.sandbox.capabilities.tools.shell_tool import ExecCommandArgs, WriteStdinArgs
from agents.sandbox.capabilities.tools.view_image import ViewImageArgs
from agents.sandbox.session.sandbox_session import SandboxSession
from agents.tool import ToolOutputImage
from pydantic import BaseModel, Field
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ActivityConfig

from temporal_agent_harness.harness.agent_workflow import Injected, activity_tool_defn, tool_activity
from temporal_agent_harness.harness.sandbox._provider import _translate_sandbox_errors
from temporal_agent_harness.harness.sandbox_image import SandboxImage

# A tool call blocks for at most the model's ``yield_time_ms`` (exec_command's own default is
# 10s) plus the time to reach the sandbox; this leaves room for slow backends. Failures the
# model caused are not retried at all (see ``_session_tool``); the rest get a few attempts, so a
# sandbox that keeps failing reaches the model as an error instead of retrying forever.
_TOOL_ACTIVITY_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(minutes=10),
    retry_policy=RetryPolicy(maximum_attempts=3),
)

# OpenAI offers apply_patch as a freeform (grammar) tool; here it is a function taking the
# patch text, so the one sentence about the freeform format is reworded.
_APPLY_PATCH_DESCRIPTION = _APPLY_PATCH_CUSTOM_TOOL_DESCRIPTION.strip().replace(
    "This is a FREEFORM tool, so do not wrap the patch in JSON.",
    "Pass the whole patch text as `patch`.",
)


class _ApplyPatchArgs(BaseModel):
    patch: str = Field(
        description=(
            "The patch, from `*** Begin Patch` to `*** End Patch`. Those two marker lines are "
            "never prefixed with `+`, even after an `*** Add File` section."
        )
    )


def _session_tool(
    name: str,
    description: str,
    args_model: type[BaseModel],
    returns: Any,
    run: Callable[[SandboxSession, Any], Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    """An activity tool named ``name`` whose model-facing parameters are ``args_model``'s
    fields, and whose body validates them into ``args_model`` and calls ``run``."""
    params = [
        inspect.Parameter(
            "session",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=Injected[SandboxSession],
        )
    ]
    annotations: dict[str, Any] = {"session": Injected[SandboxSession]}
    for field_name, info in args_model.model_fields.items():
        annotation = Annotated[  # type: ignore[valid-type]
            (info.annotation, *info.metadata, Field(description=info.description))
        ]
        params.append(
            inspect.Parameter(
                field_name,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=inspect.Parameter.empty if info.is_required() else info.get_default(),
                annotation=annotation,
            )
        )
        annotations[field_name] = annotation
    field_names = list(args_model.model_fields)

    async def body(session: SandboxSession, *values: Any) -> Any:
        # A malformed argument (ValueError/TypeError, pydantic's ValidationError included) or a
        # SandboxError OpenAI marks non-retryable (an unknown session id, a path outside the
        # workspace) fails the same way every time, so it goes straight back to the model.
        try:
            with _translate_sandbox_errors():
                return await run(
                    session, args_model(**dict(zip(field_names, values, strict=True)))
                )
        except (ValueError, TypeError) as e:
            raise ApplicationError(str(e), type=type(e).__name__, non_retryable=True) from e

    body.__name__ = body.__qualname__ = name
    body.__doc__ = description
    body.__signature__ = inspect.Signature(params, return_annotation=returns)  # type: ignore[attr-defined]
    body.__annotations__ = {**annotations, "return": returns}
    return activity_tool_defn(name=name, activity_config=_TOOL_ACTIVITY_CONFIG)(body)


async def _exec_command(session: SandboxSession, args: ExecCommandArgs) -> str:
    return await ExecCommandTool(session=session).run(args)


async def _write_stdin(session: SandboxSession, args: WriteStdinArgs) -> str:
    return await WriteStdinTool(session=session).run(args)


async def _view_image(session: SandboxSession, args: ViewImageArgs) -> SandboxImage | str:
    result = await ViewImageTool(session=session).run(args)
    if isinstance(result, ToolOutputImage) and result.image_url:
        header, _, data = result.image_url.partition(",")
        return SandboxImage(mime_type=header.removeprefix("data:").split(";")[0], data=data)
    return str(result)


async def _apply_patch(session: SandboxSession, args: _ApplyPatchArgs) -> str:
    # Models writing an `*** Add File` section often prefix the closing marker with `+` too.
    # OpenAI's parser then only says the patch must end with the marker, which models tend to
    # misread and repeat, so name the actual mistake.
    last_line = args.patch.rstrip("\n").rsplit("\n", 1)[-1]
    if last_line != "*** End Patch" and last_line.lstrip("+ ") == "*** End Patch":
        raise ValueError(
            "the patch's last line is `+*** End Patch`. It must be exactly `*** End Patch`, "
            "with no `+`: it ends the patch and is not a line of the file. Nothing was "
            "applied; resend the patch with that line fixed."
        )
    tool = SandboxApplyPatchTool(session=session)
    return await tool.on_invoke_tool(None, args.patch)  # type: ignore[arg-type]


exec_command = _session_tool(
    ExecCommandTool.tool_name,
    ExecCommandTool.tool_description,
    ExecCommandArgs,
    str,
    _exec_command,
)
write_stdin = _session_tool(
    WriteStdinTool.tool_name,
    WriteStdinTool.tool_description,
    WriteStdinArgs,
    str,
    _write_stdin,
)
view_image = _session_tool(
    ViewImageTool.tool_name,
    ViewImageTool.tool_description,
    ViewImageArgs,
    SandboxImage | str,
    _view_image,
)
apply_patch = _session_tool(
    "apply_patch", _APPLY_PATCH_DESCRIPTION, _ApplyPatchArgs, str, _apply_patch
)

_TOOLS_BY_CAPABILITY: dict[str, list[Callable[..., Awaitable[Any]]]] = {
    "shell": [exec_command, write_stdin],
    "filesystem": [view_image, apply_patch],
}

SANDBOX_TOOL_ACTIVITIES: list[Callable[..., Any]] = [
    tool_activity(t) for tools in _TOOLS_BY_CAPABILITY.values() for t in tools
]
"""The activities behind every sandbox tool, for worker registration."""


def tools_for(capabilities: Sequence[Capability]) -> list[Callable[..., Awaitable[Any]]]:
    """The harness tools the given capabilities expose to the model."""
    return [tool for c in capabilities for tool in _TOOLS_BY_CAPABILITY[c.type]]


def instructions_for(capabilities: Sequence[Capability], manifest: Manifest | None) -> str:
    """The capabilities' own prompt fragments, joined.

    ``Capability.instructions`` is async but, for the supported capabilities, never awaits
    anything, so it is driven to completion here without an event loop.
    """
    resolved = manifest if manifest is not None else Manifest()
    fragments = [_run_without_awaiting(c.instructions(resolved)) for c in capabilities]
    return "\n\n".join(f for f in fragments if f)


def _run_without_awaiting(coro: Coroutine[Any, Any, str | None]) -> str | None:
    try:
        coro.send(None)
    except StopIteration as done:
        return done.value
    coro.close()
    raise RuntimeError("capability instructions awaited I/O; they must be computed statically")
