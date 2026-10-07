"""A dev server that supports local execution, for tests whose turns run as local child workflows.

Requires:
- the temporalio-localserver package (``temporalio[local]``), with its module built;
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
