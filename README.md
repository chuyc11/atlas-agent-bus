# agent-bus

A minimal agent-to-agent message bus exposed as an **MCP stdio server**. One file, ~350 lines, Python standard library only — no dependencies.

## Why

Different AI clients (Qoder, zcode/GLM, Claude Code, ...) each run their own MCP session and have no way to talk to each other. `agent-bus` gives them a shared mailbox: every client spawns its own instance of this server, all instances read and write one JSON file on disk, so a message posted by one client becomes visible to the others.

It turns "several AI tools that cannot see each other" into a small team that can hand work back and forth.

## How it works

- **Transport** — newline-delimited JSON-RPC 2.0 over stdio (the MCP stdio transport), protocol version `2024-11-05`. Implements `initialize`, `ping`, `tools/list`, `tools/call`, and ignores `notifications/*`.
- **Identity** — per process, taken from the `AGENT_BUS_NAME` environment variable, so each client is told who it is by its own MCP config.
- **Shared state** — a single JSON mailbox at `AGENT_BUS_FILE` (default: `mailbox.json` next to the script, so two clients in the same directory share it automatically). Holds `next_id`, `messages[]`, and an `agents` last-seen map.
- **Concurrency** — cross-platform advisory lock using an `O_CREAT | O_EXCL` lock file with a 5 s timeout and 20 ms backoff; works identically on Windows and POSIX.
- **Durability** — writes are atomic: `tempfile` + `fsync` + `os.replace`, so a reader never observes a half-written mailbox.
- **Resilience** — a corrupt or truncated mailbox degrades to an empty view instead of crashing, and the next atomic write repairs it. Handler exceptions are surfaced as MCP tool errors (`isError: true`) and never kill the server; malformed JSON lines are logged and skipped.
- **Windows encoding** — stdin/stdout/stderr are reconfigured to UTF-8, because the default locale codepage (cp936 / cp1252) corrupts non-ASCII message bodies into lone surrogates and then breaks the file write.
- **Stream hygiene** — stdout carries protocol JSON only; every diagnostic goes to stderr.

## Tools

| Tool | Purpose |
| --- | --- |
| `post_message` | Post to a named agent, or `recipient: "all"` to broadcast. Returns the assigned message id. |
| `read_inbox` | Read messages addressed to an agent. `since_id` polls only new messages, `mark_read` records the reader in `read_by`, `include_broadcast` toggles `all`, `limit` caps the result. Own messages are filtered out. |
| `list_agents` | List every agent name seen on the bus, including this process. |

## Usage

Register the script as an MCP server in each client, giving each one a different name:

```json
{
  "mcpServers": {
    "agent_bus": {
      "command": "python",
      "args": ["/absolute/path/to/bus_server.py"],
      "env": { "AGENT_BUS_NAME": "qoder" }
    }
  }
}
```

A second client uses the same `args` with e.g. `AGENT_BUS_NAME: "glm-zcode"`. If the clients run from different working directories, point `AGENT_BUS_FILE` at one shared absolute path.

Typical exchange:

```
qoder     -> post_message(recipient="glm-zcode", body="...")
glm-zcode -> read_inbox(since_id=0, mark_read=true)
glm-zcode -> post_message(recipient="qoder", body="...")
```

## Status

`v0.1.0` — personal tooling, written to coordinate two agents on the same working directory, and in regular use. Single file by design; no test suite yet.
