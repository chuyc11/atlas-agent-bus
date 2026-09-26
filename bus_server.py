#!/usr/bin/env python3
"""Minimal agent-to-agent message bus exposed as an MCP stdio server.

Two (or more) MCP clients each spawn their own instance of this script.
They share state through a single JSON mailbox file on disk, so a message
posted by one client becomes visible to the others. Identity is per-process,
taken from the AGENT_BUS_NAME environment variable, so each client is told
who it is via its own MCP config.

Stdlib only. Speaks newline-delimited JSON-RPC 2.0 over stdio, which is the
MCP stdio transport. Never writes anything but protocol JSON to stdout; all
diagnostics go to stderr.

Env:
  AGENT_BUS_NAME  who this process is (e.g. "qoder" or "glm-zcode"). Required
                  for post_message; defaults to "anonymous".
  AGENT_BUS_FILE  path to the shared mailbox JSON. Defaults to mailbox.json
                  next to this script, so both clients share it automatically.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from pathlib import Path

SERVER_NAME = "agent-bus"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL = "2024-11-05"

HERE = Path(__file__).resolve().parent
BUS_FILE = Path(os.environ.get("AGENT_BUS_FILE") or (HERE / "mailbox.json"))
LOCK_FILE = BUS_FILE.with_suffix(BUS_FILE.suffix + ".lock")
SELF = os.environ.get("AGENT_BUS_NAME") or "anonymous"


# --------------------------------------------------------------------------- #
# Mailbox storage with a cross-platform advisory lock and atomic writes.
# --------------------------------------------------------------------------- #
class LockTimeout(RuntimeError):
    pass


def _acquire_lock(timeout: float = 5.0) -> None:
    """Spin on an O_CREAT|O_EXCL lock file. Works on Windows and POSIX."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            return
        except FileExistsError:
            if time.monotonic() > deadline:
                raise LockTimeout(f"could not acquire {LOCK_FILE}")
            time.sleep(0.02)


def _release_lock() -> None:
    try:
        os.unlink(str(LOCK_FILE))
    except FileNotFoundError:
        pass


def _read_unlocked() -> dict:
    if not BUS_FILE.exists():
        return {"next_id": 1, "messages": [], "agents": {}}
    try:
        with BUS_FILE.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError):
        # Corrupt or half-written file: keep going with an empty view rather
        # than crashing the server. The next atomic write repairs it.
        return {"next_id": 1, "messages": [], "agents": []}
    data.setdefault("next_id", 1)
    data.setdefault("messages", [])
    data.setdefault("agents", {})
    return data


def _write_unlocked(data: dict) -> None:
    BUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(BUS_FILE.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, str(BUS_FILE))  # atomic on both Windows and POSIX
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def with_lock(fn):
    _acquire_lock()
    try:
        return fn()
    finally:
        _release_lock()


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())


# --------------------------------------------------------------------------- #
# Tool implementations.
# --------------------------------------------------------------------------- #
def tool_post_message(args: dict) -> dict:
    recipient = (args.get("recipient") or "").strip()
    body = (args.get("body") or "").strip()
    sender = (args.get("sender") or SELF).strip()
    if not recipient:
        raise ValueError("recipient is required (use 'all' to broadcast)")
    if not body:
        raise ValueError("body is required")

    def do() -> dict:
        data = _read_unlocked()
        mid = data["next_id"]
        data["next_id"] = mid + 1
        msg = {
            "id": mid,
            "ts": _now(),
            "sender": sender,
            "recipient": recipient,
            "body": body,
            "read_by": [],
        }
        data["messages"].append(msg)
        data["agents"].setdefault(sender, {"last_seen": _now()})
        data["agents"][sender]["last_seen"] = _now()
        _write_unlocked(data)
        return msg

    msg = with_lock(do)
    return {"posted_id": msg["id"], "from": msg["sender"], "to": msg["recipient"]}


def tool_read_inbox(args: dict) -> dict:
    who = (args.get("agent") or SELF).strip()
    include_all = bool(args.get("include_broadcast", True))
    since_id = int(args.get("since_id", 0))
    mark_read = bool(args.get("mark_read", False))
    limit = int(args.get("limit", 50))

    def do() -> dict:
        data = _read_unlocked()
        out = []
        for m in data["messages"]:
            if m["id"] <= since_id:
                continue
            addressed = m["recipient"] == who or (include_all and m["recipient"] == "all")
            if not addressed or m["sender"] == who:
                continue
            out.append(m)
        if mark_read and out:
            for m in data["messages"]:
                if any(o["id"] == m["id"] for o in out) and who not in m["read_by"]:
                    m["read_by"].append(who)
            _write_unlocked(data)
        return {"agent": who, "count": len(out), "messages": out[-limit:]}

    return with_lock(do)


def tool_list_agents(_args: dict) -> dict:
    def do() -> dict:
        data = _read_unlocked()
        names = set(data.get("agents", {}).keys())
        for m in data["messages"]:
            names.add(m["sender"])
            if m["recipient"] != "all":
                names.add(m["recipient"])
        names.add(SELF)
        return {"self": SELF, "agents": sorted(n for n in names if n)}

    return with_lock(do)


TOOLS = [
    {
        "name": "post_message",
        "description": (
            "Post a message to another agent on the shared bus. Use recipient "
            "'all' to broadcast. Returns the assigned message id."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "recipient": {
                    "type": "string",
                    "description": "Target agent name, or 'all' to broadcast.",
                },
                "body": {"type": "string", "description": "Message text."},
                "sender": {
                    "type": "string",
                    "description": "Override sender name; defaults to this process's AGENT_BUS_NAME.",
                },
            },
            "required": ["recipient", "body"],
        },
    },
    {
        "name": "read_inbox",
        "description": (
            "Read messages addressed to an agent (default: this process). "
            "Includes broadcasts unless disabled. Use since_id to poll only new "
            "messages, and mark_read to record that you have seen them."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "agent": {"type": "string", "description": "Whose inbox; defaults to self."},
                "since_id": {"type": "integer", "description": "Only messages with id > this."},
                "include_broadcast": {"type": "boolean", "description": "Include recipient='all'."},
                "mark_read": {"type": "boolean", "description": "Record self in read_by."},
                "limit": {"type": "integer", "description": "Max messages to return."},
            },
        },
    },
    {
        "name": "list_agents",
        "description": "List agent names seen on the bus, including this process.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]

HANDLERS = {
    "post_message": tool_post_message,
    "read_inbox": tool_read_inbox,
    "list_agents": tool_list_agents,
}


# --------------------------------------------------------------------------- #
# JSON-RPC / MCP stdio plumbing.
# --------------------------------------------------------------------------- #
def _send(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def _result(req_id, result: dict) -> None:
    _send({"jsonrpc": "2.0", "id": req_id, "result": result})


def _error(req_id, code: int, message: str) -> None:
    _send({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}})


def handle(msg: dict) -> None:
    method = msg.get("method")
    req_id = msg.get("id")
    is_request = req_id is not None

    if method == "initialize":
        client_proto = (msg.get("params") or {}).get("protocolVersion") or DEFAULT_PROTOCOL
        if is_request:
            _result(req_id, {
                "protocolVersion": client_proto,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            })
        return

    if method == "ping":
        if is_request:
            _result(req_id, {})
        return

    if method == "tools/list":
        if is_request:
            _result(req_id, {"tools": TOOLS})
        return

    if method == "tools/call":
        params = msg.get("params") or {}
        name = params.get("name")
        args = params.get("arguments") or {}
        handler = HANDLERS.get(name)
        if handler is None:
            if is_request:
                _result(req_id, {
                    "content": [{"type": "text", "text": f"unknown tool: {name}"}],
                    "isError": True,
                })
            return
        try:
            out = handler(args)
            text = json.dumps(out, ensure_ascii=False, indent=2)
            if is_request:
                _result(req_id, {"content": [{"type": "text", "text": text}], "isError": False})
        except Exception as exc:  # surface as a tool error, keep server alive
            if is_request:
                _result(req_id, {
                    "content": [{"type": "text", "text": f"{type(exc).__name__}: {exc}"}],
                    "isError": True,
                })
        return

    if method == "notifications/initialized" or (method or "").startswith("notifications/"):
        return  # notifications get no response

    if is_request:
        _error(req_id, -32601, f"method not found: {method}")


def _force_utf8_stdio() -> None:
    # On Windows, sys.stdin/stdout default to the locale codepage (cp936/cp1252),
    # which corrupts non-ASCII message bodies into lone surrogates and then
    # crashes the UTF-8 file write. MCP clients always speak UTF-8.
    for stream, errors in ((sys.stdin, "replace"), (sys.stdout, "strict"), (sys.stderr, "replace")):
        try:
            stream.reconfigure(encoding="utf-8", errors=errors)
        except (AttributeError, ValueError):
            pass


def main() -> int:
    _force_utf8_stdio()
    sys.stderr.write(f"[agent-bus] self={SELF} bus={BUS_FILE}\n")
    sys.stderr.flush()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError as exc:
            sys.stderr.write(f"[agent-bus] bad json: {exc}\n")
            continue
        try:
            handle(msg)
        except Exception as exc:  # never let one bad message kill the server
            sys.stderr.write(f"[agent-bus] handler error: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
