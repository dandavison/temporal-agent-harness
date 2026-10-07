# Agent turns as local child workflows

A weather agent whose agent workflow runs on a Temporal server while each turn runs in the worker
process. The turn is an ordinary child workflow of the agent workflow, owned by the server, but its
workflow tasks, model calls and tool calls are served by an in-process local server (a wasm module
built from the Temporal server's code). The local server syncs the turn's history to the server
once a second and when the turn ends, so the Temporal UI shows the turn while it runs.

This is a prototype. It needs a server that supports local execution and an SDK that can run a
local server, both on unmerged branches (below).

## Layout

| File | Role |
|---|---|
| `workflow.py` | `LocalTurnsAgentWorkflow`, the agent workflow; `WeatherTurn`, one turn: the OpenAI Agents SDK loop with a `get_weather` tool that takes 3 seconds. |
| `demo.py` | Runs the agent workflow's worker and the local turns worker, starts an agent, and chats with it in the terminal. |
| `temporal_agent_harness/local_turns.py` | `run_local_turn`, which starts a turn as a child workflow, and `LocalTurns`, the local server and its worker. |

## Build

You need Go, Rust, `uv`, and the wasmtime CLI at the version the SDK's local-server host uses
(48): a compiled module only loads in the wasmtime version and features that compiled it.

1. **The local-server module**, from a checkout of
   [dandavison/temporalio-temporal:chasm-standalone-build-libs](https://github.com/dandavison/temporalio-temporal/tree/chasm-standalone-build-libs):

   ```sh
   GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -ldflags="-s -w" -trimpath -o /tmp/local-server.wasm ./cmd/chasmwasm
   wasmtime compile -W gc=n,gc-support=n,concurrency-support=n,threads=n,shared-everything-threads=n,component-model=n,exceptions=n,stack-switching=n /tmp/local-server.wasm -o /tmp/local-server.cwasm
   ```

2. **A Temporal CLI whose dev server supports local execution.** Check out, side by side in one
   directory:
   - `temporal`: [dandavison/temporalio-temporal:local-first-child](https://github.com/dandavison/temporalio-temporal/tree/local-first-child) (Spencer Judge's `sj/local-first-execution`, plus a fix for lease expiry);
   - `temporal-api-go`: temporalio/api-go `sj/local-first-execution` (the server's `go.mod` replaces `go.temporal.io/api` with `../temporal-api-go`);
   - `cli`: temporalio/cli `sj/local-first-execution`.

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

3. **The Python SDK.** This repo's `pyproject.toml` takes `temporalio` from
   `../../../sdk-python/local-workflow-progress/sdk-python`, a checkout of
   [dandavison/temporalio-sdk-python:local-workflow-progress](https://github.com/dandavison/temporalio-sdk-python/tree/local-workflow-progress)
   whose `temporalio/bridge/sdk-core` submodule is at the commit the branch records, from
   [dandavison/temporalio-sdk-core:local-workflow-progress](https://github.com/dandavison/temporalio-sdk-core/tree/local-workflow-progress).
   Build its bridge from that checkout, in release mode (a debug build is about 15 times slower):

   ```sh
   uv run maturin develop --uv --release
   ```

## Run

In one terminal, start the dev server built in step 2, with local execution enabled:

```sh
./temporal server start-dev --dynamic-config-value history.enableLocalExecution=true
```

In another, from this repo's root:

```sh
OPENAI_API_KEY=... TEMPORAL_LOCAL_SERVER_MODULE=/tmp/local-server.cwasm \
  uv run --group examples python -m examples.local_turns_agent.demo
```

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
  worker connected to the server can continue it, and the demo has none.
- Compared with running the loop in the agent workflow, a turn has no tool approvals, callback
  tools, tool events or streamed reply deltas: those go through the agent workflow.
