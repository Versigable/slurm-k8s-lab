"""Drill engine: registry, the Run context drills act through, results and the scorecard.
See drill.py for the CLI and catalog.py for the drills."""

import datetime as dt
import json
import subprocess
import time
import traceback
from pathlib import Path

import lab

RESULTS = Path(__file__).resolve().parent / "results"
SCORECARD = Path(__file__).resolve().parent / "SCORECARD.md"
REGISTRY = {}


class Drill:
    def __init__(self, name, title, fn, minutes, needs, teaches):
        self.name, self.title, self.fn, self.minutes, self.needs, self.teaches = name, title, fn, minutes, needs, teaches
        self.doc = " ".join((fn.__doc__ or "").split())


def drill(name, title, *, minutes, needs=(), teaches=""):
    def register(fn):
        REGISTRY[name] = Drill(name, title, fn, minutes, tuple(needs), teaches)
        return fn
    return register


class DrillTimeout(Exception):
    pass


def now_iso():
    return dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")


class Run:
    """What a drill uses to act and to record what happened."""

    def __init__(self, d, capture_dir=None, log=None):
        self.drill = d
        self.t0 = time.monotonic()
        self.events, self.decisions, self.checks, self.cleanups = [], [], [], []
        self.capture_dir = capture_dir
        self.log = log
        self.triage_version = ""

    def elapsed(self):
        return round(time.monotonic() - self.t0, 1)

    def mark(self, kind, text, **data):
        ev = {"t": self.elapsed(), "at": now_iso(), "kind": kind, "text": text}
        if data:
            ev["data"] = data
        self.events.append(ev)
        extra = ""
        if data:
            extra = "  " + json.dumps(data, default=str)[:300]
        print(f"  [{ev['t']:7.1f}s] {kind:8} {text}{extra}", flush=True)
        return ev

    def note(self, text, **data):
        return self.mark("note", text, **data)

    def wait(self, text, fn, timeout, every=2.0, kind="observe", required=True):
        """Poll fn until it returns something truthy. Transient errors (a dead
        host, a restarting API) count as "not yet"."""
        start, last_err = time.monotonic(), None
        while True:
            try:
                v = fn()
            except (lab.CommandError, json.JSONDecodeError, KeyError, IndexError, ValueError) as e:
                v, last_err = None, str(e)[-300:]
            if v:
                self.mark(kind, text, waited=round(time.monotonic() - start, 1))
                return v
            if time.monotonic() - start > timeout:
                data = {"last_error": last_err} if last_err else {}
                self.mark("timeout" if required else "note", f"{text}: not within {timeout}s", **data)
                if required:
                    raise DrillTimeout(text)
                return None
            time.sleep(every)

    def check(self, text, ok, detail=None):
        self.checks.append({"t": self.elapsed(), "text": text, "ok": bool(ok), "detail": detail})
        self.mark("check", ("PASS " if ok else "FAIL ") + text, **({"detail": detail} if detail is not None else {}))
        return bool(ok)

    def decide(self, node, expect, label, category=None):
        """Ask node-triage about one node and grade the answer."""
        res = lab.TRIAGE.call("triage_node", {"node": node})
        self.triage_version = res.version or self.triage_version
        rec = res.data
        ok = rec is not None and rec.get("action") in expect and (category is None or rec.get("category") == category)
        got = None if rec is None else {k: rec.get(k) for k in ("action", "severity", "category", "summary")}
        d = {"t": self.elapsed(), "label": label, "node": node, "ok": ok, "latency": res.latency,
             "expected": {"action": list(expect), "category": category}, "got": got, "error": res.error,
             "recommendation": rec}
        self._capture(d, label)
        self.decisions.append(d)
        shown = f"{got['action']}/{got['severity']}/{got['category']}" if got else f"ERROR {res.error}"
        self.mark("decide", f"{label}: {node} -> {shown} (want {'|'.join(expect)}"
                  f"{'/' + category if category else ''}) {'OK' if ok else 'GAP'}",
                  **({"summary": got["summary"]} if got else {}))
        return rec

    def decide_cluster(self, label, grade):
        """Ask for triage_cluster. grade(data) returns (ok, why) for a successful call;
        an error is a GAP unless grade_error says otherwise."""
        res = lab.TRIAGE.call("triage_cluster")
        self.triage_version = res.version or self.triage_version
        if res.data is not None:
            ok, why = grade(res.data)
            recs = [{k: r.get(k) for k in ("node", "cluster", "action", "severity", "category", "summary")}
                    for r in res.data.get("recommendations", [])]
        else:
            ok, why, recs = False, "node-triage returned an error instead of a triage", None
        d = {"t": self.elapsed(), "label": label, "node": "*", "ok": ok, "latency": res.latency,
             "expected": {"note": grade.__doc__ or ""}, "got": recs, "error": res.error, "why": why}
        self._capture(d, label)
        self.decisions.append(d)
        self.mark("decide", f"{label}: triage_cluster -> {'ERROR ' + res.error if res.error else str(len(recs)) + ' recommendation(s)'}"
                  f" {'OK' if ok else 'GAP'}", why=why)
        return res.data

    def _capture(self, d, label):
        if self.capture_dir:
            slug = "".join(c if c.isalnum() else "-" for c in label.lower()).strip("-")
            try:
                d["fixture"] = str(lab.capture_fixtures(self.capture_dir / slug))
            except Exception as e:  # capture is best effort; never fail a drill on it
                d["fixture_error"] = str(e)

    def cleanup(self, text, fn):
        self.cleanups.append((text, fn))


def first(events, kind):
    return next((e["t"] for e in events if e["kind"] == kind), None)


def metrics(events):
    inject, detect, fix, recover = (first(events, k) for k in ("inject", "detect", "fix", "recover"))
    sub = lambda a, b: round(b - a, 1) if a is not None and b is not None else None  # noqa: E731
    return {"detect_s": sub(inject, detect), "fix_to_recover_s": sub(fix, recover), "inject_to_recover_s": sub(inject, recover)}


def git_rev():
    res = subprocess.run(["git", "describe", "--always", "--dirty"], cwd=lab.REPO, capture_output=True, text=True)
    return res.stdout.strip()


def run_drill(d, args):
    print(f"\n==> {d.name}: {d.title}\n    {d.doc}", flush=True)
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    RESULTS.mkdir(exist_ok=True)
    capture = (RESULTS / f"{stamp}_{d.name}_fixtures") if args.capture else None
    r = Run(d, capture_dir=capture, log=RESULTS / f"{stamp}_{d.name}.log")
    result = {"drill": d.name, "title": d.title, "teaches": d.teaches, "started": now_iso(), "repo": git_rev()}

    missing = [n for n in d.needs if not NEEDS[n]()]
    if missing:
        print(f"    SKIP: lab lacks {', '.join(missing)}")
        return None
    if not args.force:
        probs = lab.lab_problems()
        if probs:
            print("    SKIP: the lab isn't green before the drill:\n      " + "\n      ".join(probs))
            return None

    status, error = "PASS", None
    try:
        d.fn(r)
    except DrillTimeout as e:
        status, error = "FAIL", f"timed out: {e}"
    except KeyboardInterrupt:
        status, error = "FAIL", "interrupted"
        r.mark("note", "interrupted; cleaning up")
    except Exception as e:  # report and still clean up
        status, error = "FAIL", f"{type(e).__name__}: {e}"
        r.mark("error", error, trace=traceback.format_exc()[-1500:])

    for text, fn in reversed(r.cleanups):
        try:
            fn()
        except Exception as e:
            r.mark("note", f"cleanup '{text}' failed: {e}")
    green = r.wait("lab back to green", lambda: not lab.lab_problems(), timeout=900, every=10, kind="green", required=False)
    if not green:
        status = "FAIL"
        r.note("still not green", problems=lab.lab_problems())

    if status == "PASS" and not all(c["ok"] for c in r.checks):
        status = "FAIL"
    if status == "PASS" and not all(d_["ok"] for d_ in r.decisions):
        status = "GAP"
    result.update({
        "finished": now_iso(), "status": status, "error": error, "duration_s": r.elapsed(),
        "triage_version": r.triage_version, "metrics": metrics(r.events),
        "events": r.events, "checks": r.checks, "decisions": r.decisions,
    })
    out = RESULTS / f"{stamp}_{d.name}.json"
    out.write_text(json.dumps(result, indent=1, default=str) + "\n")
    m = result["metrics"]
    print(f"<== {d.name}: {status} in {r.elapsed():.0f}s (detect {m['detect_s']}s, fix->recover {m['fix_to_recover_s']}s)"
          f"{' - ' + error if error else ''}\n    {out}", flush=True)
    return result


# --- capability probes for drills that need optional lab features ------------------------

def _has(host, cmd):
    try:
        return lab.ssh(host, cmd, check=False).rc == 0
    except Exception:
        return False


NEEDS = {
    "nhc": lambda: _has("slurm-c1", "test -x /usr/sbin/nhc && test -x /usr/local/sbin/lab-burnin"),
    "reboot": lambda: _has("slurm-ctl", "scontrol show config | grep -q '^RebootProgram *= */'"),
    "bringup": lambda: "extra_compute_nodes" in (lab.REPO / "terraform" / "variables.tf").read_text(),
}


# --- report ------------------------------------------------------------------------------

def fmt_s(v):
    if v is None:
        return "-"
    return f"{v:.0f} s" if v < 120 else f"{v / 60:.1f} min"


def decision_word(d):
    if d["error"]:
        return "error"
    if isinstance(d["got"], dict):
        return d["got"]["action"]
    return "cluster view"


def report():
    latest = {}
    for f in sorted(RESULTS.glob("*.json")):
        r = json.loads(f.read_text())
        latest[r["drill"]] = r
    lines = [
        "# Drill scorecard",
        "",
        "Generated by `drills/drill.py report` from the latest run of each drill in `drills/results/`.",
        "**Detect**: fault in until the scheduler noticed. **Recover**: fix in until the node/service was back.",
        "**Total**: fault in until back. **Decisions**: node-triage's answers at each decision point, graded.",
        "",
        "| Drill | Result | Detect | Recover | Total | Decisions | Ran |",
        "|---|---|---|---|---|---|---|",
    ]
    for name in REGISTRY:
        r = latest.get(name)
        if not r:
            lines.append(f"| {name} | not run | | | | | |")
            continue
        m = r["metrics"]
        dec = "; ".join(f"{d['label']}: {decision_word(d)} {'✓' if d['ok'] else '✗'}" for d in r["decisions"]) or "-"
        lines.append(f"| {name} | **{r['status']}** | {fmt_s(m['detect_s'])} | {fmt_s(m['fix_to_recover_s'])} | "
                     f"{fmt_s(m['inject_to_recover_s'])} | {dec} | {r['started'][:16].replace('T', ' ')} |")
    gaps = [(n, d) for n, r in latest.items() for d in r["decisions"] if not d["ok"]]
    if gaps:
        lines += ["", "## Open decision gaps", ""]
        for n, d in gaps:
            got = d["got"]["summary"] if isinstance(d["got"], dict) else (d["error"] or d.get("why"))
            lines.append(f"- **{n} / {d['label']}** ({d['node']}): wanted {d['expected']}, got: {got}")
    SCORECARD.write_text("\n".join(lines) + "\n")
    print(SCORECARD.read_text())


