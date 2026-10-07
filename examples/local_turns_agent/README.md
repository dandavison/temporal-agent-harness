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

## Build

You need Go, Rust and `uv`.

1. **A Temporal CLI whose dev server supports local execution.** Check out, side by side in one
   directory:
   - `temporal`: [dandavison/temporalio-temporal:local-chasm-spencer-wf-lease](https://github.com/dandavison/temporalio-temporal/tree/local-chasm-spencer-wf-lease) (Spencer Judge's `sj/local-first-execution`, plus a fix for lease expiry);
   - `temporal-api-go`: [dandavison/temporalio-api-go:local-chasm](https://github.com/dandavison/temporalio-api-go/tree/local-chasm) (the server's `go.mod` replaces `go.temporal.io/api` with `../temporal-api-go`);
   - `cli`: [dandavison/temporalio-cli:local-chasm](https://github.com/dandavison/temporalio-cli/tree/local-chasm).

   Then, in that directory, create a `go.work` and build:

   ```sh
   cat > go.work <<'EOF'
   go 1.26.5

   use (
   	./cli
   	./cli/cliext
   )

   replace (
   	go.temporal.io/server => ./temporal
   	go.temporal.io/api => ./temporal-api-go
   )
   EOF
   (cd cli && go build -o ../temporal ./cmd/temporal)
   ```

2. **The Python SDK and the local-server module.** This repo's `pyproject.toml` takes `temporalio`,
   and the `temporalio-localserver` package that holds the module, from
   `../../../sdk-python/local-workflow-progress/sdk-python`, a checkout of
   [dandavison/temporalio-sdk-python:local-chasm](https://github.com/dandavison/temporalio-sdk-python/tree/local-chasm)
   whose `temporalio/bridge/sdk-core` submodule is at the commit the branch records, from
   [dandavison/temporalio-sdk-core:local-chasm](https://github.com/dandavison/temporalio-sdk-core/tree/local-chasm).
   In that checkout, build the module into `temporalio-localserver` from a checkout of
   [dandavison/temporalio-temporal:local-chasm](https://github.com/dandavison/temporalio-temporal/tree/local-chasm),
   and build the bridge in release mode (a debug build is about 15 times slower):

   ```sh
   scripts/build-local-server-module <temporal local-chasm checkout>
   uv run maturin develop --uv --release
   ```

## Run

In one terminal, start the dev server built in step 1, with local execution enabled:

```sh
./temporal server start-dev --dynamic-config-value history.enableLocalExecution=true
```

In another, from this repo's root:

```sh
OPENAI_API_KEY=... uv run --group examples python -m examples.local_turns_agent.demo
```

The first run compiles the module, which takes about 2 seconds and 1.8 GB of memory on an M-series
Mac; the compiled code is cached, and later runs load it in about 0.2 seconds.

The demo prints the agent workflow's URL in the Temporal UI (http://localhost:8233). Ask, for
example, "What's the weather in Boston?". The agent's model is `MODEL` in `workflow.py`.

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
