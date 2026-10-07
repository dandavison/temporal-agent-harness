# Modal coding agent

A Gemini coding agent with one durable Modal sandbox (`docs/design/agent-sandboxes.md`). It gets
the full sandbox tool suite from `runner.sandbox_tools()`:

| Tool | Capability | What it does |
|---|---|---|
| `exec_command` | Shell | Runs a command on a PTY. A command still running when it returns (a server, a long job) gives back a session id. |
| `write_stdin` | Shell | Checks back on a running session (empty input) or types into it, in this turn or a later one. |
| `apply_patch` | Filesystem | Edits files with a patch. |
| `view_image` | Filesystem | Reads an image from the sandbox and shows it to the model. |

The sandbox is created on the agent's first tool call, saved and shut down after `IDLE_AFTER`
without a message, restored on the next message, and deleted when the session closes. Files
survive an idle shutdown; running processes do not.

## Layout

| File | Role |
|---|---|
| `workflow.py` | `ModalCodingAgent`: the `SandboxConfig`, the system prompt, and the Gemini Interactions tool loop. |
| `worker.py` | Worker hosting the workflow, the `modal` provider's sandbox activities, and the sandbox tool activities. |
| `agents.toml` | Registry entry that makes the agent selectable in the web UI. |

The knobs are constants at the top of `workflow.py`:

| Constant | Default | Meaning |
|---|---|---|
| `IDLE_AFTER` | 10 min | Idle time before the workspace is saved and the sandbox shut down. |
| `MODAL_SANDBOX_TIMEOUT` | 1 h | Modal's hard lifetime for one sandbox, from creation. |
| `SNAPSHOT_DIR` | `/tmp/harness-modal-sandbox-snapshots` | Where the worker keeps workspace snapshots (tar files). |

Snapshots are files on the worker's disk, and running processes live in the worker's memory, so
run exactly one worker.

## Run it

Prereqs, in the repo-root `.env.local`: the usual Temporal connection, `GEMINI_API_KEY`, and a
Modal token (`modal token new` writes `~/.modal.toml`, or set `MODAL_TOKEN_ID`/`MODAL_TOKEN_SECRET`).
Then, from this directory, each in its own terminal:

```sh
just temporal          # 1. local Temporal dev server (or bring your own)
just session-manager   # 2. packaged session-manager worker
just server            # 3. API + UI on http://localhost:8000
just worker            # 4. the agent's worker
```

Open http://localhost:8000 and pick **Modal Coding Agent**. If it isn't listed, the session
manager is still running with an older agent list: `temporal workflow terminate -w
session-manager`, then restart `just session-manager` and `just server`.

The recipes add the Modal SDK with `--with 'modal==1.6.1'` rather than the `openai-agents[modal]`
extra, which pins `modal==1.4.3`; on 1.4.3, `Sandbox.exec` hangs, and the agent's first sandbox
start with it.

## What to try

- *"Start a Python script that prints a counter every 2 seconds and leave it running."* Then, in
  a later message: *"Check on the counter."* It should poll the same session with `write_stdin`.
- *"Start `python3` interactively and compute 2**100 in it."* Typing into a TTY.
- *"Write a small Flask app, run it in the background, and curl it."*
- *"Plot a sine wave to a PNG and look at it."* `view_image`.
- Leave it idle past `IDLE_AFTER`, then ask about the counter: the session is gone (processes
  don't survive the shutdown), but the files it wrote are still there.

Watch the workflow's history in the Temporal UI (http://localhost:8233): the sandbox lifecycle
activities are `modal-sandbox_client_create`/`_resume`, `modal-sandbox_session_start`/`_stop`/
`_shutdown`, `modal-sandbox_client_delete` and `modal-sandbox_snapshot_delete`. The sandboxes
themselves show up in the Modal dashboard under the app `temporal-agent-harness-sandbox`.

## Not handled yet

Modal killing the sandbox while it's in use (reaching `MODAL_SANDBOX_TIMEOUT`) is not recovered:
there is no save before it, and rebuilding a lost sandbox isn't built (see the design doc's "Box
lost mid-run"). To try it, set `MODAL_SANDBOX_TIMEOUT` below `IDLE_AFTER`.
