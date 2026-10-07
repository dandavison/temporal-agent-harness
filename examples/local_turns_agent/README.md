# Agent turns as local child workflows

A weather agent whose agent workflow runs on a Temporal server while each turn runs in the worker
process. The turn is an ordinary child workflow of the agent workflow, owned by the server. Its
task queue's worker has local execution (`Worker(..., local_execution=LocalExecution())`), so the
turn's workflow tasks, model calls and tool calls are served by an in-process local server (a wasm
module built from the Temporal server's code). The local server syncs the turn's history to the
server once a second and when the turn ends, so the Temporal UI shows the turn while it runs.

This is a prototype. It needs a server that supports local execution and an SDK that can run a
local server, both on unmerged branches (below).

## Layout

| File | Role |
|---|---|
| `workflow.py` | `LocalTurnsAgentWorkflow`, the agent workflow; `WeatherTurn`, one turn: the OpenAI Agents SDK loop with a `get_weather` tool that takes 3 seconds. |
| `demo.py` | Runs the agent workflow's worker and the turn worker, which has local execution; starts an agent, and chats with it in the terminal. |

## Build and run

You need git, Go, Rust (rustup) and uv. In an empty directory:

```sh
git clone --depth 1 --branch local-chasm https://github.com/dandavison/temporal-agent-harness
temporal-agent-harness/examples/local_turns_agent/build-demo
```

[`build-demo`](build-demo) clones the other branches next to `temporal-agent-harness` and builds
them:

| Directory | Branch | Built |
|---|---|---|
| `server/temporal`, `server/temporal-api-go`, `server/cli` | `local-chasm-spencer-wf-lease` of [dandavison/temporalio-temporal](https://github.com/dandavison/temporalio-temporal/tree/local-chasm-spencer-wf-lease) (Spencer Judge's `sj/local-first-execution`, plus a fix for lease expiry); `local-chasm` of [dandavison/temporalio-api-go](https://github.com/dandavison/temporalio-api-go/tree/local-chasm) and [dandavison/temporalio-cli](https://github.com/dandavison/temporalio-cli/tree/local-chasm) | `bin/temporal`: a Temporal CLI whose dev server supports local execution |
| `temporal` | `local-chasm` of [dandavison/temporalio-temporal](https://github.com/dandavison/temporalio-temporal/tree/local-chasm) | the local-server wasm module, into `sdk-python/temporalio-localserver` |
| `sdk-python` | `local-chasm` of [dandavison/temporalio-sdk-python](https://github.com/dandavison/temporalio-sdk-python/tree/local-chasm), with its `sdk-core` submodule from [dandavison/temporalio-sdk-core](https://github.com/dandavison/temporalio-sdk-core/tree/local-chasm) | the SDK's bridge, installed in this repo's environment |

Then start the dev server, with local execution enabled:

```sh
bin/temporal server start-dev --dynamic-config-value history.enableLocalExecution=true
```

and, in another terminal, the demo:

```sh
cd temporal-agent-harness
OPENAI_API_KEY=... uv run --group examples python -m examples.local_turns_agent.demo
```

The demo prints the agent workflow's URL in the Temporal UI (http://localhost:8233). Ask, for
example, "What's the weather in Boston?". The agent's model is `MODEL` in `workflow.py`.

The first run compiles the module, which takes about 2 seconds and 1.8 GB of memory on an M-series
Mac; the compiled code is cached, and later runs load it in about 0.2 seconds.

## What to look at in the UI

- The agent workflow (`LocalTurnsAgent`) has no activities. Each turn is a child workflow
  (`WeatherTurn`), linked from the agent workflow's history.
- Open the turn while `get_weather` is running (it takes 3 seconds): the turn is Running, and its
  history shows the model call and the scheduled tool call. The rest appears when the turn ends.
- The turn's history has the usual workflow task and activity events, written by the local server
  and accepted by the server as the turn's history.

## Limitations

- While the local server owns a turn, the UI's query for the turn's metadata fails, and so do
  other queries, signals, updates and cancellation of the turn: the server does not yet relay them
  to the local server.
- If the demo process dies mid-turn, the server takes the turn back after 3 seconds, but only a
  worker without local execution can continue it, and the demo has none: a local server can take
  ownership only of new runs.
- Compared with running the loop in the agent workflow, a turn has no tool approvals, callback
  tools, tool events or streamed reply deltas: those go through the agent workflow.
