"""Reaching the lab from the control node: SSH, classic Slurm, Slinky,
Kubernetes, Proxmox power, Terraform/Ansible, and node-triage over MCP.

Standard library only, so the drills run anywhere the rebuild does.
"""

import json
import os
import shlex
import ssl
import subprocess
import threading
import time
import urllib.request
from collections import namedtuple
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
KEY = os.path.expanduser(os.environ.get("LAB_SSH_KEY", "~/.ssh/slurm-lab"))
USER = "labadmin"
PVE_API = os.environ.get("PVE_API", "https://10.0.2.1:8006/api2/json")
PVE_NODE = os.environ.get("PVE_NODE", "rogue")
PVE_TOKEN_FILE = os.path.expanduser(os.environ.get("PVE_TOKEN_FILE", "~/.config/slurm-k8s-lab/pve-token"))

HOSTS = {
    "slurm-ctl": "10.0.5.130",
    "slurm-c1": "10.0.5.131",
    "slurm-c2": "10.0.5.132",
    "k8s-cp": "10.0.5.133",
    "k8s-w1": "10.0.5.134",
    "k8s-w2": "10.0.5.135",
    "slurm-c3": "10.0.5.136",  # only exists during the node-bringup drill
}
VMIDS = {"slurm-c1": 131, "slurm-c2": 132, "slurm-c3": 136, "k8s-w1": 134, "k8s-w2": 135}

# Slurm state flags that mean "not taking work".
OUT_OF_SERVICE = {
    "DOWN", "DRAIN", "NOT_RESPONDING", "FAIL", "INVALID", "INVALID_REG", "REBOOT_REQUESTED",
    "REBOOT_ISSUED", "MAINTENANCE", "POWERED_DOWN", "POWERING_DOWN", "FUTURE", "RESERVED",
}

Result = namedtuple("Result", "rc out err")


class CommandError(Exception):
    def __init__(self, what, res):
        super().__init__(f"{what}: rc={res.rc}: {(res.err or res.out).strip()[-400:]}")
        self.res = res


def ssh_argv(host, cmd):
    return ["ssh", "-i", KEY, "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
            # Trust on first use, on our own LAN (as rebuild.sh does): the drills
            # create and destroy slurm-c3, which gets a new host key each time.
            "-o", "StrictHostKeyChecking=accept-new", "-o", "LogLevel=ERROR",
            f"{USER}@{HOSTS.get(host, host)}", cmd]


def run(argv, *, check=True, timeout=120, stdin=None, env=None, cwd=None, what=None):
    io = {"input": stdin} if stdin is not None else {"stdin": subprocess.DEVNULL}
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, env=env, cwd=cwd, **io)
        res = Result(p.returncode, p.stdout, p.stderr)
    except subprocess.TimeoutExpired as e:
        res = Result(-1, e.stdout or "", f"timed out after {timeout}s")
    if check and res.rc != 0:
        raise CommandError(what or " ".join(argv[-1:]), res)
    return res


def ssh(host, cmd, **kw):
    kw.setdefault("what", f"{host}: {cmd}")
    return run(ssh_argv(host, cmd), **kw)


# --- classic Slurm and Slinky -------------------------------------------------------

def slurm(cmd, **kw):
    """A command on the classic cluster's controller."""
    return ssh("slurm-ctl", cmd, **kw)


def slinky(cmd, **kw):
    """A command inside Slinky's slurmctld container."""
    kw.setdefault("what", f"slinky: {cmd}")
    return ssh("k8s-cp", "kubectl -n slurm exec slurm-controller-0 -c slurmctld -- " + cmd, **kw)


def node(sched, name):
    """State flags, reason and partitions of one Slurm node."""
    n = json.loads(sched(f"scontrol show node {name} --json").out)["nodes"][0]
    return {"state": set(n["state"]), "reason": n.get("reason") or "", "partitions": n.get("partitions") or []}


def state(sched, name):
    return node(sched, name)["state"]


def in_service(flags):
    return bool(flags & {"IDLE", "MIXED", "ALLOCATED"}) and not flags & OUT_OF_SERVICE


def sbatch(sched, args, sudo=False):
    """Submit a batch job; returns its id."""
    cmd = f"sbatch --parsable {args}"
    out = sched(("sudo " if sudo else "") + cmd).out.strip()
    return int(out.split(";")[0])


def job(sched, jobid):
    """Live view of a job, or None once slurmctld has forgotten it."""
    res = sched(f"scontrol show job {jobid} --json", check=False)
    if res.rc != 0:
        return None
    j = json.loads(res.out)["jobs"][0]
    return {"state": set(j["job_state"]), "nodes": j.get("nodes") or "", "restarts": j.get("restart_cnt", 0),
            "reason": j.get("state_reason") or ""}


def running_on(sched, jobid, nodename):
    j = job(sched, jobid)
    return bool(j) and "RUNNING" in j["state"] and j["nodes"] == nodename


def accounted(jobid):
    """State and exit code from accounting (classic cluster only), or None."""
    res = slurm(f"sacct -n -X -P -j {jobid} -o State,ExitCode,NodeList", check=False)
    line = res.out.strip().splitlines()[:1]
    if res.rc != 0 or not line:
        return None
    st, code, nodes = line[0].split("|")
    return {"state": st.split()[0], "exit": code, "nodes": nodes}


def drill_jobs(sched):
    """(id, name, user, state) of jobs the drills submitted."""
    out = sched('squeue -h -o "%i|%j|%u|%T"', check=False).out
    jobs = [tuple(line.split("|")) for line in out.splitlines() if line.count("|") == 3]
    return [j for j in jobs if j[1].startswith("drill-") or j[1] == "burnin"]


def admin(sched, cmd, **kw):
    """An administrative Slurm command: via sudo on the classic controller; the
    Slinky controller container already runs as root and has no sudo."""
    return sched(("sudo " if sched is slurm else "") + cmd, **kw)


def cancel_drill_jobs(sched):
    ids = [j[0] for j in drill_jobs(sched)]
    if ids:
        admin(sched, "scancel " + " ".join(ids), check=False)


# --- Kubernetes -------------------------------------------------------------------

def kubectl(args, **kw):
    kw.setdefault("what", f"kubectl {args}")
    return ssh("k8s-cp", "kubectl " + args, **kw)


def kjson(args):
    return json.loads(kubectl(args + " -o json").out)


def k8s_node(name):
    n = kjson(f"get node {name}")
    return {
        "conditions": {c["type"]: c["status"] for c in n["status"].get("conditions", [])},
        "unschedulable": bool(n["spec"].get("unschedulable")),
        "taints": [f'{t["key"]}:{t["effect"]}' for t in n["spec"].get("taints", [])],
    }


def pods(selector="", namespace="-A"):
    ns = namespace if namespace == "-A" else f"-n {namespace}"
    sel = f"-l {shlex.quote(selector)}" if selector else ""
    out = []
    for p in kjson(f"get pods {ns} {sel}")["items"]:
        out.append({
            "name": p["metadata"]["name"],
            "namespace": p["metadata"]["namespace"],
            "node": p["spec"].get("nodeName", ""),
            "phase": p["status"].get("phase", ""),
            "terminating": "deletionTimestamp" in p["metadata"],
            "ready": all(c.get("ready") for c in p["status"].get("containerStatuses", [])) and bool(p["status"].get("containerStatuses")),
            "labels": p["metadata"].get("labels", {}),
        })
    return out


def apply_manifest(yaml_text):
    kubectl("apply -f -", stdin=yaml_text)


# --- Proxmox ------------------------------------------------------------------------

def pve_power(name, action):
    """start | stop (hard power-off) | shutdown | reboot one lab VM.

    Uses the pool-scoped Terraform token, so a drill can only ever touch lab VMs.
    The token goes in a request header from this process, never on a command line.
    """
    token = Path(PVE_TOKEN_FILE).read_text().strip()
    req = urllib.request.Request(
        f"{PVE_API}/nodes/{PVE_NODE}/qemu/{VMIDS[name]}/status/{action}", data=b"", method="POST",
        headers={"Authorization": f"PVEAPIToken={token}"})
    ctx = ssl.create_default_context()
    ctx.check_hostname, ctx.verify_mode = False, ssl.CERT_NONE  # Proxmox's self-signed cert, lab LAN
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        return json.load(r)


# --- Terraform and Ansible ------------------------------------------------------------

def tool_env():
    env = dict(os.environ)
    env["PROXMOX_VE_API_TOKEN"] = Path(PVE_TOKEN_FILE).read_text().strip()
    env["ANSIBLE_CONFIG"] = str(REPO / "ansible" / "ansible.cfg")
    env["ANSIBLE_PRIVATE_KEY_FILE"] = KEY
    env["ANSIBLE_NOCOLOR"] = "1"
    return env


def terraform_apply(var_file=None, log=None):
    argv = ["terraform", "apply", "-auto-approve", "-input=false", "-no-color"]
    if var_file:
        argv.append(f"-var-file={var_file}")
    res = run(argv, cwd=REPO / "terraform", env=tool_env(), timeout=900, check=False)
    _log(log, "terraform apply", res)
    if res.rc != 0:
        raise CommandError("terraform apply", res)
    return res


def ansible(playbook, limit=None, log=None):
    argv = ["ansible-playbook", playbook]
    if limit:
        argv += ["--limit", limit]
    res = run(argv, cwd=REPO / "ansible", env=tool_env(), timeout=1800, check=False)
    _log(log, f"ansible-playbook {playbook}", res)
    if res.rc != 0:
        raise CommandError(f"ansible-playbook {playbook}", res)
    recap = res.out[res.out.rfind("PLAY RECAP"):].strip().splitlines()[1:]
    return [" ".join(line.split()) for line in recap]


def _log(path, title, res):
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"\n===== {title} (rc={res.rc}) =====\n{res.out}\n{res.err}\n")


# --- node-triage over MCP ---------------------------------------------------------------

TriageResult = namedtuple("TriageResult", "data error latency version")


class Triage:
    """node-triage reached exactly the way Claude Code reaches it: MCP over SSH stdio,
    read-only (no -allow-writes)."""

    CMD = "/usr/local/bin/node-triage -tz America/Denver"

    def call(self, tool, args=None, timeout=90):
        t0 = time.monotonic()
        p = subprocess.Popen(ssh_argv("slurm-ctl", self.CMD), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True)
        timer = threading.Timer(timeout, p.kill)
        timer.start()
        version, resp = "", None
        try:
            def send(msg):
                p.stdin.write(json.dumps(msg) + "\n")
                p.stdin.flush()

            def recv(want):
                for line in p.stdout:
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if msg.get("id") == want:
                        return msg
                return None

            send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "lab-drills", "version": "1"}}})
            init = recv(1)
            if init and "result" in init:
                version = init["result"].get("serverInfo", {}).get("version", "")
                send({"jsonrpc": "2.0", "method": "notifications/initialized"})
                send({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                      "params": {"name": tool, "arguments": args or {}}})
                resp = recv(2)
        except (BrokenPipeError, OSError):
            pass
        finally:
            timer.cancel()
            try:
                p.stdin.close()
            except OSError:
                pass
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        latency = round(time.monotonic() - t0, 2)
        if resp is None:
            err = (p.stderr.read() or "").strip()[-400:] if p.stderr else ""
            return TriageResult(None, f"no answer from node-triage: {err or 'timed out'}", latency, version)
        if "error" in resp:
            return TriageResult(None, resp["error"].get("message", str(resp["error"])), latency, version)
        res = resp["result"]
        if res.get("isError"):
            text = " ".join(c.get("text", "") for c in res.get("content", []))
            return TriageResult(None, text, latency, version)
        return TriageResult(res.get("structuredContent"), None, latency, version)


TRIAGE = Triage()


# --- is the lab green? -------------------------------------------------------------------

def _sinfo_problems(sched, label):
    res = sched('sinfo -h -N -o "%N %T %E"', check=False)
    if res.rc != 0:
        return [f"{label}: sinfo failed: {(res.err or res.out).strip()[-200:]}"]
    probs = []
    for line in sorted(set(res.out.splitlines())):
        parts = line.split(None, 2)
        if len(parts) >= 2 and parts[1] not in ("idle", "mixed", "allocated"):
            probs.append(f"{label}: {parts[0]} is {parts[1]} ({parts[2] if len(parts) > 2 else ''})")
    probs += [f"{label}: leftover drill job {j[0]} {j[1]} ({j[3]})" for j in drill_jobs(sched)]
    return probs


def lab_problems(with_triage=True):
    """Everything that isn't green, as human-readable lines. Empty list = green."""
    probs = _sinfo_problems(slurm, "classic")
    probs += _sinfo_problems(slinky, "slinky")
    try:
        nodes = kjson("get nodes")["items"]
        for n in nodes:
            name = n["metadata"]["name"]
            for c in n["status"].get("conditions", []):
                want = "True" if c["type"] == "Ready" else "False"
                if c["status"] != want:
                    probs.append(f"kubernetes: {name} {c['type']}={c['status']}")
            if n["spec"].get("unschedulable"):
                probs.append(f"kubernetes: {name} is cordoned")
        for p in pods():
            if p["namespace"] == "gitlab-ci" and p["name"].startswith("runner-"):
                continue  # CI job pods come and go with pipelines; the runner itself is checked
            if p["phase"] not in ("Running", "Succeeded") or p["terminating"]:
                probs.append(f"kubernetes: pod {p['namespace']}/{p['name']} {p['phase']}{' terminating' if p['terminating'] else ''}")
        if kubectl("get namespace drill", check=False).rc == 0:
            probs.append("kubernetes: drill namespace still exists")
        for a in kjson("-n argocd get applications")["items"]:
            st = a.get("status", {})
            sync, health = st.get("sync", {}).get("status"), st.get("health", {}).get("status")
            if (sync, health) != ("Synced", "Healthy"):
                probs.append(f"argo cd: {a['metadata']['name']} {sync}/{health}")
    except (CommandError, json.JSONDecodeError) as e:
        probs.append(f"kubernetes: {e}")
    if with_triage and not probs:
        res = TRIAGE.call("triage_cluster")
        if res.error:
            probs.append(f"node-triage: {res.error}")
        elif res.data.get("unavailable"):
            probs += [f"node-triage: {u['cluster']} {u['component']} unavailable: {u['error']}" for u in res.data["unavailable"]]
        elif res.data["healthy_nodes"] != res.data["total_nodes"]:
            for rec in res.data["recommendations"]:
                probs.append(f"node-triage: {rec['node']} {rec['action']}: {rec['summary']}")
    return probs


# --- fixture capture (so a drill's decision point becomes a regression test) ------------

def _trim(kind, items):
    """Keep only the fields triage reads, like triage/scripts/capture-k8s-scenarios.sh."""
    out = []
    for o in items:
        meta = o["metadata"]
        keep = {k: meta[k] for k in ("name", "namespace", "labels", "ownerReferences") if k in meta}
        if kind == "nodes":
            keep["annotations"] = {k: v for k, v in meta.get("annotations", {}).items() if k.startswith("slurm-k8s-lab/")}
            st = o["status"]
            out.append({"metadata": keep,
                        "spec": {k: o["spec"][k] for k in ("unschedulable", "taints") if k in o["spec"]},
                        "status": {"conditions": st.get("conditions", []),
                                   "nodeInfo": {"kubeletVersion": st["nodeInfo"]["kubeletVersion"]}}})
        elif kind == "pods":
            keep["annotations"] = {k: v for k, v in meta.get("annotations", {}).items() if k == "kubernetes.io/config.mirror"}
            if "deletionTimestamp" in meta:
                keep["deletionTimestamp"] = meta["deletionTimestamp"]
            out.append({"metadata": keep, "spec": {"nodeName": o["spec"].get("nodeName", "")},
                        "status": {"phase": o["status"].get("phase", "")}})
        else:
            out.append(o)
    return {"items": out}


def _last_line(res):
    text = (res.err or res.out).strip()
    return text.splitlines()[-1] if text else f"exit status {res.rc}"


def _save(d, fname, command, res):
    """A command's output, or <command>.err with its error: the fixture runners
    replay that as the command failing, the way it failed during the drill."""
    if res.rc == 0:
        (d / fname).write_text(res.out)
    else:
        (d / f"{command}.err").write_text(_last_line(res))


def capture_fixtures(dest):
    """Snapshot what node-triage reads, in the format its fixture runners replay."""
    dest = Path(dest)
    now = slurm("date +%s").out
    classic = dest / "slurm"
    classic.mkdir(parents=True, exist_ok=True)
    for fname, cmd in (("nodes.json", "scontrol show node --json"), ("squeue.json", "squeue --json"),
                       ("sacct.json", "sacct --json -a -S now-1days"),
                       ("events.txt", "sacctmgr -n -P show event event=node start=now-1days format=NodeName,TimeStart,TimeEnd,State,Reason,User")):
        _save(classic, fname, cmd.split()[0], slurm(cmd, check=False))
    (classic / "now.txt").write_text(now)
    kube = dest / "kube"
    kube.mkdir(exist_ok=True)
    for kind, args in (("nodes", "get nodes"), ("pods", "get pods -A"),
                       ("events", "get events -A --field-selector involvedObject.kind=Node"), ("pdbs", "get pdb -A")):
        res = kubectl(args + " -o json", check=False)
        if res.rc == 0:
            (kube / f"{kind}.json").write_text(json.dumps(_trim(kind, json.loads(res.out)["items"]), indent=1))
        else:
            (kube / f"{kind}.err").write_text(_last_line(res))
    (kube / "now.txt").write_text(now)
    sl = dest / "slinky"
    sl.mkdir(exist_ok=True)
    for fname, cmd in (("nodes.json", "scontrol show node --json"), ("squeue.json", "squeue --json")):
        _save(sl, fname, cmd.split()[0], slinky(cmd, check=False))
    (sl / "sacct.json").write_text('{"jobs":[]}')
    (sl / "events.txt").write_text("")
    (sl / "now.txt").write_text(now)
    return dest
