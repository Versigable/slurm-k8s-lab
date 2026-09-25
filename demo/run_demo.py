#!/usr/bin/env python3
"""Record a live demo of the node-triage MCP server driven by Claude Code.

What it does, against the real lab cluster:
  1. Breaks slurm-c1 the way a GPU falling off the bus does (slurmd registers
     3 of 4 GPUs, slurmctld marks the node INVALID).
  2. Asks Claude Code (headless, `claude -p`) what needs attention.
  3. Asks it to put slurm-c1 back into service (the server's hardware guard).
  4. Approves a maintenance drain of slurm-c2 and shows the result in sinfo.
  5. Restores both nodes.

Claude gets only the slurm-triage MCP tools (no shell, no file access), with
skills/triage-slurm-node/SKILL.md appended as its instructions. Everything on
screen is real output; the recording is written as an asciicast v2 file
(wall-clock timings; retime.py makes the watchable version).

Usage (from the repo root, with the lab up and node-triage deployed):
  python demo/run_demo.py [--key ~/.ssh/claude-agent] [--out demo/node-triage-demo.raw.cast]
  python demo/retime.py demo/node-triage-demo.raw.cast demo/node-triage-demo.cast --height 48
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CTL, C1, C2 = "10.0.5.130", "10.0.5.131", "10.0.5.132"
WIDTH, HEIGHT = 110, 36

RESET, BOLD, DIM = "\x1b[0m", "\x1b[1m", "\x1b[2m"
CYAN, GREEN, YELLOW, RED, MAGENTA = "\x1b[36m", "\x1b[32m", "\x1b[33m", "\x1b[31m", "\x1b[35m"
SEVERITY_COLOR = {"critical": RED, "warning": YELLOW, "info": GREEN}


class Cast:
    """Minimal asciicast v2 recorder that also echoes to the real terminal."""

    def __init__(self, path: Path, title: str):
        self.path, self.start = path, time.monotonic()
        self.f = path.open("w", encoding="utf-8", newline="\n")
        header = {"version": 2, "width": WIDTH, "height": HEIGHT, "timestamp": int(time.time()),
                  "idle_time_limit": 2.5, "title": title, "env": {"TERM": "xterm-256color", "SHELL": "/bin/bash"}}
        self.f.write(json.dumps(header) + "\n")

    def out(self, text: str):
        text = text.replace("\r\n", "\n").replace("\n", "\r\n")
        self.f.write(json.dumps([round(time.monotonic() - self.start, 3), "o", text]) + "\n")
        self.f.flush()
        sys.stdout.write(text)
        sys.stdout.flush()

    def line(self, text: str = ""):
        self.out(text + "\n")

    def type(self, text: str, prompt: str = f"{GREEN}${RESET} ", delay: float = 0.025):
        self.out(prompt)
        for ch in text:
            self.out(ch)
            time.sleep(delay)
        self.out("\n")
        time.sleep(0.3)

    def pause(self, seconds: float):
        time.sleep(seconds)

    def close(self):
        self.f.close()


def ssh(key: str, host: str, command: str, check: bool = True) -> str:
    ssh_bin = shutil.which("ssh") or "ssh"
    res = subprocess.run(
        [ssh_bin, "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-o", "LogLevel=ERROR",
         "-i", key, f"labadmin@{host}", command],
        capture_output=True, text=True, timeout=120)
    if check and res.returncode != 0:
        raise RuntimeError(f"ssh {host} {command!r}: {res.stderr.strip()}")
    return res.stdout


def wait_for(key: str, node: str, pattern: str, timeout: int = 90) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = ssh(key, CTL, f"scontrol show node {node}", check=False)
        if re.search(pattern, state):
            return True
        time.sleep(3)
    return False


def summarize_tool_result(payload) -> list[str]:
    """Turn a node-triage structured result into a few readable lines."""
    lines = []
    if isinstance(payload, dict) and "recommendations" in payload:
        lines.append(f"{payload.get('total_nodes')} nodes, {payload.get('healthy_nodes')} healthy")
        for r in payload["recommendations"]:
            color = SEVERITY_COLOR.get(r["severity"], "")
            head = f"{r['severity'].upper():8} {r['node']:9} {r['action']}  "
            summary = textwrap.shorten(r["summary"], WIDTH - 8 - len(head), placeholder="…")
            lines.append(f"{color}{r['severity'].upper():8}{RESET} {r['node']:9} {BOLD}{r['action']}{RESET}  {summary}")
    elif isinstance(payload, dict) and "action" in payload:
        color = SEVERITY_COLOR.get(payload["severity"], "")
        lines.append(f"{color}{payload['severity'].upper()}{RESET} {payload['node']}: {BOLD}{payload['action']}{RESET} ({payload['category']})")
        lines += [f"- {e}" for e in payload.get("evidence", [])[:4]]
    elif isinstance(payload, dict) and "running_jobs" in payload:
        n = payload["node"]
        lines.append(f"{n['name']}: {'+'.join(n['state'])}  reason: {n.get('reason') or '-'}"
                     f"{' (set by ' + n['reason_set_by'] + ')' if n.get('reason_set_by') else ''}")
        lines.append(f"{len(payload['running_jobs'])} running job(s), {len(payload['recent_jobs'])} recent, "
                     f"{len(payload['events'])} drain/down event(s)")
    elif isinstance(payload, dict) and "done" in payload:
        mark = f"{GREEN}done{RESET}" if payload["done"] else f"{YELLOW}not done{RESET}"
        lines.append(f"{mark}  {payload.get('command', '')}")
        if payload.get("note"):
            lines.append(payload["note"])
    else:
        text = payload if isinstance(payload, str) else json.dumps(payload)
        lines.append(text[: WIDTH - 12] + ("…" if len(text) > WIDTH - 12 else ""))
    return lines


def ask(cast: Cast, claude: list[str], prompt: str, session: str | None, workdir: Path) -> str | None:
    cast.line()
    cast.type(prompt, prompt=f"{MAGENTA}{BOLD}you ›{RESET} ", delay=0.02)
    cmd = claude + ["-p", prompt] + (["--resume", session] if session else [])
    proc = subprocess.Popen(cmd, cwd=workdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                            encoding="utf-8", errors="replace")
    session_id = session
    for raw in proc.stdout:
        try:
            ev = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if ev.get("type") == "assistant":
            for block in ev["message"].get("content", []):
                if block.get("type") == "tool_use":
                    name = block["name"].split("__")[-1]
                    cast.line(f"  {CYAN}● {name}{RESET}{DIM}({json.dumps(block.get('input', {}))}){RESET}")
                elif block.get("type") == "text" and block.get("text", "").strip():
                    cast.line()
                    for para in block["text"].strip().split("\n"):
                        wrapped = textwrap.wrap(para, WIDTH - 4) or [""]
                        for w in wrapped:
                            cast.line(f"  {w}")
        elif ev.get("type") == "user":
            for block in ev["message"].get("content", []):
                if block.get("type") != "tool_result":
                    continue
                content = block.get("content")
                text = content if isinstance(content, str) else "".join(
                    c.get("text", "") for c in content or [] if c.get("type") == "text")
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    payload = text
                for ln in summarize_tool_result(payload):
                    cast.line(f"    {DIM}└{RESET} {ln}")
        elif ev.get("type") == "result":
            session_id = ev.get("session_id", session_id)
    proc.wait()
    if proc.returncode != 0:
        cast.line(f"{RED}claude exited {proc.returncode}: {proc.stderr.read().strip()[:300]}{RESET}")
    return session_id


def break_gpu(key: str):
    ssh(key, C1, "sudo rm -f /dev/fakegpu3 && sudo cp /etc/slurm/gres.conf /etc/slurm/gres.conf.orig && "
                 "sudo sed -i '/^NodeName=slurm-c1 /s/fakegpu\\[0-3\\]/fakegpu[0-2]/' /etc/slurm/gres.conf && "
                 "sudo systemctl restart slurmd")


def restore(key: str):
    ssh(key, C1, "test -f /etc/slurm/gres.conf.orig && sudo mv /etc/slurm/gres.conf.orig /etc/slurm/gres.conf; "
                 "sudo systemd-tmpfiles --create /etc/tmpfiles.d/fakegpu.conf && sudo systemctl restart slurmd", check=False)
    time.sleep(10)
    for node in ("slurm-c1", "slurm-c2"):
        ssh(key, CTL, f"sudo scontrol update nodename={node} state=resume", check=False)
    time.sleep(3)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--key", default=os.path.expanduser("~/.ssh/claude-agent"), help="SSH key authorized for labadmin")
    ap.add_argument("--out", default=str(REPO / "demo" / "node-triage-demo.raw.cast"))
    ap.add_argument("--model", default=None, help="Claude model for the headless session (default: CLI default)")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")  # the Windows console default can't print ● or └

    claude_bin = shutil.which("claude")
    if not claude_bin:
        sys.exit("claude CLI not found on PATH")
    ssh_bin = shutil.which("ssh") or "ssh"

    # The demo server has writes enabled; the everyday one does not.
    mcp_config = {"mcpServers": {"slurm-triage": {
        "command": ssh_bin,
        "args": ["-T", "-o", "BatchMode=yes", "-i", args.key, f"labadmin@{CTL}",
                 "/usr/local/bin/node-triage", "-allow-writes", "-tz", "America/Denver"]}}}
    # Run Claude from an empty directory: from inside the repo, Claude Code puts
    # the git status in its context and the demo starts commenting on it.
    workdir = Path(tempfile.mkdtemp(prefix="node-triage-demo-"))
    cfg = workdir.parent / f"{workdir.name}-mcp.json"
    cfg.write_text(json.dumps(mcp_config), encoding="utf-8")
    skill = (REPO / "skills" / "triage-slurm-node" / "SKILL.md").read_text(encoding="utf-8")

    claude = [claude_bin, "--output-format", "stream-json", "--verbose",
              "--strict-mcp-config", "--mcp-config", str(cfg),
              "--tools", "", "--allowedTools", "mcp__slurm-triage", "--permission-mode", "dontAsk",
              "--append-system-prompt", skill]
    if args.model:
        claude += ["--model", args.model]

    restore(args.key)  # start from a clean cluster
    if not wait_for(args.key, "slurm-c1", r"State=IDLE\s") or not wait_for(args.key, "slurm-c2", r"State=IDLE\s"):
        sys.exit("cluster isn't clean (both computes IDLE); fix it before recording")

    cast = Cast(Path(args.out), "node-triage: Claude Code triaging a real Slurm failure over MCP")
    try:
        cast.line(f"{BOLD}node-triage demo{RESET}  {DIM}Claude Code + a Go MCP server on a real 3-node Slurm lab{RESET}")
        cast.line(f"{DIM}Claude has only the slurm-triage tools: no shell, no files. SKILL.md is its playbook.{RESET}")
        cast.line()
        cast.line(f"{BOLD}1. A GPU falls off the bus on slurm-c1{RESET} {DIM}(slurmd now finds 3 of 4 GPUs){RESET}")
        cast.type("ssh slurm-c1 'rm /dev/fakegpu3; <gres.conf lists 3 devices>; systemctl restart slurmd'")
        break_gpu(args.key)
        cast.out(f"{DIM}  waiting for slurmctld to register it{RESET}")
        for _ in range(30):
            if wait_for(args.key, "slurm-c1", r"INVALID_REG", timeout=3):
                break
            cast.out(".")
        cast.line()
        cast.type("sinfo -N -o '%N %T %E'")
        cast.out(ssh(args.key, CTL, "sinfo -N -p batch -o '%N %T %E'"))
        cast.pause(1.5)

        cast.line()
        cast.line(f"{BOLD}2. Ask Claude{RESET}")
        session = ask(cast, claude, "Anything in the Slurm cluster need attention?", None, workdir)
        cast.pause(1.5)

        cast.line()
        cast.line(f"{BOLD}3. Try to put it back{RESET}")
        session = ask(cast, claude, "Resume slurm-c1, I approve.", session, workdir)
        cast.pause(1.5)

        cast.line()
        cast.line(f"{BOLD}4. An approved maintenance drain{RESET}")
        session = ask(cast, claude, "Drain slurm-c2 for CHG-1043, a BIOS update tonight. I approve.", session, workdir)
        cast.line()
        cast.type("sinfo -N -o '%N %T %E'")
        cast.out(ssh(args.key, CTL, "sinfo -N -p batch -o '%N %T %E'"))
        cast.pause(3)
    finally:
        cast.close()
        print(f"\n{DIM}restoring the cluster...{RESET}")
        restore(args.key)
        print(ssh(args.key, CTL, "sinfo -N -p batch -o '%N %T %E'", check=False))
        print(f"recording: {args.out}")


if __name__ == "__main__":
    main()
