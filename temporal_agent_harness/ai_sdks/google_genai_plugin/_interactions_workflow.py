"""Workflow-side helpers for the Interactions API.

Exports :func:`function_param` — a small helper that turns a tool callable into an
Interactions-API ``FunctionParam`` dict so the workflow can declare it as a tool — and
:func:`function_result_value`, which turns the tool's return value into a function result. Pass a tool defined with :func:`harness.agent.activity_tool_defn` or
:func:`harness.agent.tool_defn`; the workflow drives the tool-calling loop itself,
executing each call via :meth:`~harness.agent_workflow.AgentWorkflowRunner.run_tool`.

The actual ``interactions.create`` call is made through the Temporal-aware
:class:`~.TemporalAsyncInteractions` shim on the workflow's
:func:`google_genai_client` — workflow code just uses
``gemini.interactions.create(...)`` directly, no plugin-specific
dispatcher required.
"""

from __future__ import annotations

import inspect
from typing import Any

from google.genai.types import FunctionDeclaration
from google.genai._interactions.types import FunctionParam

from temporal_agent_harness.harness.sandbox_image import SandboxImage

# The image types a Gemini function result may carry (``ImageContentParam.mime_type``).
_GEMINI_IMAGE_MIME_TYPES = frozenset(
    {
        "image/png",
        "image/jpeg",
        "image/webp",
        "image/heic",
        "image/heif",
        "image/gif",
        "image/bmp",
        "image/tiff",
    }
)


def function_param(fn: Any) -> FunctionParam:
    """Build an Interactions-API ``FunctionParam`` dict from a tool callable.

    Introspects ``fn``'s signature via Gemini's
    :meth:`FunctionDeclaration.from_callable_with_api_option`, converts the
    resulting Gemini ``Schema`` into a JSON Schema dict, and wraps it as
    a ``{"type": "function", ...}`` tool declaration suitable for the
    ``tools=`` argument of ``client.interactions.create(...)``.

    Pass a tool defined with :func:`harness.agent.activity_tool_defn` /
    :func:`harness.agent.tool_defn`. Those decorators expose a MODEL-FACING signature on
    the returned object (``self`` and ``Injected[...]`` parameters already stripped, no
    misleading ``__wrapped__``), so introspecting it directly yields exactly the schema
    the model should see. The tool's ``__name__`` becomes the tool name the model emits.
    """
    decl = FunctionDeclaration.from_callable_with_api_option(
        callable=fn, api_option="GEMINI_API",
    )
    if decl.parameters_json_schema is not None:
        parameters: Any = decl.parameters_json_schema
    elif decl.parameters is not None:
        parameters = decl.parameters.json_schema.model_dump(
            by_alias=True, exclude_none=True, mode="json",
        )
    else:
        parameters = {"type": "object"}
    return {
        "type": "function",
        "name": fn.__name__,
        "description": inspect.cleandoc(fn.__doc__) if fn.__doc__ else "",
        "parameters": parameters,
    }


def function_result_value(result: Any) -> Any:
    """The ``result`` of a ``function_result`` step for a tool's return value.

    A :class:`~temporal_agent_harness.harness.sandbox_image.SandboxImage` (what a sandbox
    ``view_image`` tool returns) becomes an image content block, so the model sees the
    image; an image type Gemini does not accept, and every other value, becomes its
    ``str()``::

        {"type": "function_result", "call_id": call.id, "name": call.name,
         "result": function_result_value(await runner.run_tool(call.id, tool, **call.arguments))}
    """
    if isinstance(result, SandboxImage) and result.mime_type in _GEMINI_IMAGE_MIME_TYPES:
        return [{"type": "image", "data": result.data, "mime_type": result.mime_type}]
    return str(result)
