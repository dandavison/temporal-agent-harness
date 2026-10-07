"""A dev server that supports local execution, for tests whose turns run as local child workflows.

Requires:
- TEMPORAL_LOCAL_SERVER_MODULE: the path of a precompiled local-server module (.cwasm).
- TEMPORAL_LOCAL_EXECUTION_CLI: a Temporal CLI built against a server that supports local
  execution (temporalio/temporal branch sj/local-first-execution).
"""

import os
from typing import Any

from temporalio.testing import WorkflowEnvironment


async def start_local_execution_server(**kwargs: Any) -> WorkflowEnvironment:
    return await WorkflowEnvironment.start_local(
        dev_server_existing_path=os.environ["TEMPORAL_LOCAL_EXECUTION_CLI"],
        dev_server_extra_args=[
            "--dynamic-config-value",
            "history.enableLocalExecution=true",
        ],
        **kwargs,
    )


def local_server_module() -> str:
    return os.environ["TEMPORAL_LOCAL_SERVER_MODULE"]


def target_host(env: WorkflowEnvironment) -> str:
    return env.client.service_client.config.target_host
