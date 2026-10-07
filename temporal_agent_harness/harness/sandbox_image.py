"""The image a sandbox ``view_image`` tool returns.

A leaf module, free of the optional ``agents`` dependency, so the model SDK adapters can
recognize the result without the sandbox extra installed.
"""

from __future__ import annotations

from pydantic import BaseModel


class SandboxImage(BaseModel):
    """An image read from the sandbox, as base64 ``data`` of type ``mime_type``.

    Its ``str()`` is a short placeholder rather than the payload, since that is what the tool
    events carry and what an adapter without image support hands the model.
    """

    mime_type: str
    data: str

    def __str__(self) -> str:
        return f"[image: {self.mime_type}, {len(self.data) * 3 // 4} bytes]"
