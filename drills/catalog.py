"""The drills. Each one is a function that breaks one thing, records what the
schedulers and node-triage do about it, fixes it, and registers the cleanup
that puts the lab back however far the drill got.

Expectations in r.decide() are what a good compute-production engineer would
do at that moment, not what node-triage happened to say when the drill was
written: a mismatch is a finding (GAP), and the fix goes into node-triage.
"""

import json
import re
import subprocess
import time

import lab
from engine import drill

CLASSIC, SLINKY = lab.slurm, lab.slinky
NHC_INTERVAL = 60  # HealthCheckInterval in slurm.conf.j2
TOTAL_NODES = 7  # 2 classic + 2 Slinky + 3 Kubernetes


def safe(fn):
    """For observations that may be impossible mid-fault (e.g. kubectl exec into a
    pod whose kubelet is down): record why instead of failing the drill."""
    try:
        return fn()
    except Exception as e:
        return f"unavailable: {str(e)[-160:]}"


def classic(node):
    return lab.state(CLASSIC, node)


def slinky_state(node):
    return lab.state(SLINKY, node)


def wait_running(r, sched, jobid, node, what):
    return r.wait(f"{what} (job {jobid}) running on {node}", lambda: lab.running_on(sched, jobid, node), 90)


def drained_by(node, prefix):
    n = lab.node(CLASSIC, node)
    return "DRAIN" in n["state"] and n["reason"].startswith(prefix) and n


def resume_if_out(sched, node):
    try:
        if not lab.in_service(lab.state(sched, node)):
            lab.admin(sched, f"scontrol update nodename={node} state=resume", check=False)
    except lab.CommandError:
        pass


def cluster_ok(data):
    """No error, every node of every cluster triaged."""
    return data["total_nodes"] >= TOTAL_NODES, f"{data['total_nodes']} nodes triaged"


DRILL_NS = """apiVersion: v1
kind: Namespace
metadata: {name: drill}
"""


def drill_deployment(name, replicas, pdb_min_available=None):
    """nginx pods spread one-per-worker; tainted nodes (control plane, cordoned,
    unreachable) don't count as spread domains."""
    yaml = DRILL_NS + f"""---
apiVersion: apps/v1
kind: Deployment
metadata: {{name: {name}, namespace: drill}}
spec:
  replicas: {replicas}
  selector: {{matchLabels: {{app: {name}}}}}
  template:
    metadata: {{labels: {{app: {name}}}}}
    spec:
      topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: kubernetes.io/hostname
        whenUnsatisfiable: DoNotSchedule
        nodeTaintsPolicy: Honor
        labelSelector: {{matchLabels: {{app: {name}}}}}
      containers:
      - name: web
        image: nginx:stable-alpine
        resources: {{requests: {{cpu: 10m, memory: 16Mi}}}}
"""
    if pdb_min_available is not None:
        yaml += f"""---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {{name: {name}, namespace: drill}}
spec:
  minAvailable: {pdb_min_available}
  selector: {{matchLabels: {{app: {name}}}}}
"""
    lab.apply_manifest(yaml)
    lab.kubectl(f"-n drill rollout status deploy/{name} --timeout=180s", timeout=200)


def placement(app):
    out = {}
    for p in lab.pods(f"app={app}", "drill"):
        state = "terminating" if p["terminating"] else p["phase"]
        out.setdefault(p["node"] or "unscheduled", []).append(state)
    return out


def delete_drill_ns():
    lab.kubectl("delete namespace drill --wait=true --timeout=180s", check=False, timeout=200)


def slurm_pod_where(podname):
    for p in lab.pods(namespace="slurm"):
        if p["name"] == podname:
            return p
    return None


# --- classic Slurm ---------------------------------------------------------------------

@drill("node-death", "Compute node loses power mid-job", minutes=10,
       teaches="SlurmdTimeout is the price of not false-alarming: jobs sit on a dead node until it expires, and a "
               "requeued job waits another ~2 minutes. Requeue-safe jobs come back elsewhere; the rest end NODE_FAIL. "
               "A node that rebooted unasked comes back held DOWN for a human.")
def node_death(r):
    """slurm-c2 is hard powered off (Proxmox stop, no shutdown) while it runs two
    jobs, one requeue-safe and one not."""
    r.cleanup("cancel drill jobs", lambda: lab.cancel_drill_jobs(CLASSIC))
    r.cleanup("power slurm-c2 on", lambda: lab.pve_power("slurm-c2", "start"))
    # Fill slurm-c1 so both jobs land on slurm-c2, then free it so the requeued
    # job has somewhere to go.
    blocker = lab.sbatch(CLASSIC, "-w slurm-c1 --exclusive --mem=200M -J drill-blocker --wrap 'sleep 600'")
    wait_running(r, CLASSIC, blocker, "slurm-c1", "blocker")
    safe = lab.sbatch(CLASSIC, "--requeue -n1 --mem=200M -J drill-requeue-safe --wrap 'sleep 900'")
    unsafe = lab.sbatch(CLASSIC, "--no-requeue -n1 --mem=200M -J drill-no-requeue --wrap 'sleep 900'")
    wait_running(r, CLASSIC, safe, "slurm-c2", "requeue-safe job")
    wait_running(r, CLASSIC, unsafe, "slurm-c2", "no-requeue job")
    CLASSIC(f"scancel {blocker}")

    lab.pve_power("slurm-c2", "stop")
    r.mark("inject", "slurm-c2 powered off mid-job (hard stop: no shutdown, no warning)")
    r.wait("slurmctld flags slurm-c2 NOT_RESPONDING", lambda: "NOT_RESPONDING" in classic("slurm-c2"),
           400, every=3, kind="detect")
    r.decide("slurm-c2", ["investigate"], "not responding, jobs still listed")
    r.wait("slurm-c2 marked DOWN (SlurmdTimeout=300s)", lambda: "DOWN" in classic("slurm-c2"), 600, every=3)
    r.wait(f"requeue-safe job {safe} running again on slurm-c1", lambda: lab.running_on(CLASSIC, safe, "slurm-c1"), 180)
    acct = r.wait(f"no-requeue job {unsafe} recorded", lambda: lab.accounted(unsafe), 60)
    r.check(f"no-requeue job {unsafe} ended NODE_FAIL", acct["state"] == "NODE_FAIL", acct)
    r.decide("slurm-c2", ["investigate", "escalate_hardware"], "down")

    j = lab.job(CLASSIC, safe)
    r.note("requeued job", restarts=j["restarts"], node=j["nodes"],
           eligible_after_requeue=CLASSIC(f"sacct -j {safe} --duplicates -X -n -P -o Eligible,Reason | tail -1", check=False).out.strip())

    lab.pve_power("slurm-c2", "start")
    r.mark("observe", "slurm-c2 powered back on")
    # ReturnToService=1 only returns nodes that went DOWN for not responding. One
    # that rebooted without being asked is held DOWN for a human: a surprise
    # reboot is a hardware signal (crash, power, watchdog).
    n = r.wait("slurm-c2 registers but is held DOWN", lambda: (x := lab.node(CLASSIC, "slurm-c2")) and "DOWN" in x["state"]
               and "NOT_RESPONDING" not in x["state"] and x, 300, every=3)
    r.note("why it's held", reason=n["reason"])
    r.decide("slurm-c2", ["investigate"], "back after an unexpected reboot", category="unexpected_reboot")
    r.note("diagnosis: end of the previous boot's log (no shutdown sequence = power loss or hard hang)",
           lines=lab.ssh("slurm-c2", "sudo journalctl -b -1 -n 3 --no-pager -o short-iso", check=False).out.strip().splitlines())
    CLASSIC("sudo scontrol update nodename=slurm-c2 state=resume")
    r.mark("fix", "cause understood (power loss); slurm-c2 resumed")
    r.wait("slurm-c2 back in service", lambda: lab.in_service(classic("slurm-c2")), 60, every=2, kind="recover")


@drill("maintenance-reboot", "Rolling kernel update: drain, reboot, return", minutes=4, needs=("reboot",),
       teaches="scontrol reboot ASAP nextstate=RESUME is a whole maintenance workflow in one command; "
               "triage must read REBOOT_* states as planned work, not an outage.")
def maintenance_reboot(r):
    """`scontrol reboot ASAP nextstate=RESUME` on slurm-c1 while a job runs: Slurm
    drains it, lets the job finish, reboots it and returns it to service."""
    node = "slurm-c1"
    r.cleanup("cancel drill jobs", lambda: lab.cancel_drill_jobs(CLASSIC))
    r.cleanup("cancel a pending reboot", lambda: CLASSIC(f"sudo scontrol cancel_reboot {node}", check=False))
    r.cleanup(f"resume {node}", lambda: resume_if_out(CLASSIC, node))
    boot_before = lab.ssh(node, "cat /proc/sys/kernel/random/boot_id").out.strip()
    victim = lab.sbatch(CLASSIC, f"-w {node} -t 3 --mem=200M -J drill-maint-running --wrap 'sleep 60'")
    wait_running(r, CLASSIC, victim, node, "running job")

    CLASSIC(f'sudo scontrol reboot ASAP nextstate=RESUME reason="maint: kernel update CHG-3001" {node}')
    r.mark("inject", "maintenance requested: scontrol reboot ASAP nextstate=RESUME")
    r.wait("draining, reboot pending", lambda: {"DRAIN", "REBOOT_REQUESTED"} <= classic(node), 30, every=1, kind="detect")
    r.decide(node, ["wait"], "reboot pending, job running")
    newjob = lab.sbatch(CLASSIC, "-n1 --mem=200M -J drill-maint-newwork --wrap 'sleep 5'")
    acct = r.wait(f"new job {newjob} ran", lambda: (a := lab.accounted(newjob)) and a["state"] == "COMPLETED" and a, 90)
    r.check(f"new work avoided {node}", acct["nodes"] != node, acct)
    acct = r.wait(f"job {victim} finished normally", lambda: (a := lab.accounted(victim)) and a["state"] != "RUNNING" and a, 120)
    r.check(f"job {victim} completed, not killed", acct["state"] == "COMPLETED", acct)
    r.wait("reboot issued", lambda: "REBOOT_ISSUED" in classic(node) or "DOWN" in classic(node), 60, every=1)
    r.decide(node, ["wait"], "rebooting")
    r.wait(f"{node} back in service with no human step", lambda: lab.in_service(classic(node)), 300, every=2, kind="recover")
    boot_after = lab.ssh(node, "cat /proc/sys/kernel/random/boot_id").out.strip()
    r.check(f"{node} really rebooted", boot_after != boot_before)


@drill("nhc-disk-full", "Health check catches a full disk", minutes=4, needs=("nhc",),
       teaches="NHC drains on a transient fault and resumes the node itself once the check passes, "
               "because the drain reason is NHC's own.")
def nhc_disk_full(r):
    """The root filesystem on slurm-c2 fills to 95%; NHC requires 10% free."""
    node = "slurm-c2"
    r.cleanup("remove the fill file", lambda: lab.ssh(node, "sudo rm -f /var/tmp/drill-fill", check=False))
    r.cleanup(f"resume {node}", lambda: resume_if_out(CLASSIC, node))
    size, avail = map(int, lab.ssh(node, "df -B1 --output=size,avail / | tail -1").out.split())
    fill = avail - size * 5 // 100
    lab.ssh(node, f"sudo fallocate -l {fill} /var/tmp/drill-fill")
    r.mark("inject", f"/ on {node} filled to 95% ({fill / 2**30:.1f} GiB fallocate)")
    n = r.wait("NHC drains it", lambda: drained_by(node, "NHC"), NHC_INTERVAL * 3, kind="detect")
    r.note("drain reason", reason=n["reason"])
    r.decide(node, ["investigate"], "drained by NHC: disk", category="health_check")
    lab.ssh(node, "sudo rm -f /var/tmp/drill-fill")
    r.mark("fix", "fill file removed")
    r.wait("NHC resumes it by itself", lambda: lab.in_service(classic(node)), NHC_INTERVAL * 3, kind="recover")


@drill("nhc-gpu-lost", "GPU falls off the bus: escalate, repair, burn in, return", minutes=9, needs=("nhc",),
       teaches="A health check can find hardware faults; those must not auto-resume after repair. "
               "Take the drain away from NHC, repair, burn in behind a reservation, then return.")
def nhc_gpu_lost(r):
    """/dev/fakegpu3 disappears from slurm-c1. NHC drains the node; triage should
    call it hardware. The drain is re-labelled so NHC can't auto-resume it, the
    GPU is "replaced", and the node is burned in behind a maintenance
    reservation before it rejoins batch."""
    node = "slurm-c1"
    r.cleanup("restore the GPU device", lambda: lab.ssh(node, "sudo systemd-tmpfiles --create /etc/tmpfiles.d/fakegpu.conf", check=False))
    r.cleanup("delete the burn-in reservation", lambda: CLASSIC("sudo scontrol delete reservation=drill-burnin", check=False))
    r.cleanup("cancel drill jobs", lambda: lab.cancel_drill_jobs(CLASSIC))
    r.cleanup(f"resume {node}", lambda: resume_if_out(CLASSIC, node))

    lab.ssh(node, "sudo rm -f /dev/fakegpu3")
    r.mark("inject", "/dev/fakegpu3 vanished (GPU fell off the bus)")
    n = r.wait("NHC drains it", lambda: drained_by(node, "NHC"), NHC_INTERVAL * 3, kind="detect")
    r.note("drain reason", reason=n["reason"])
    r.decide(node, ["escalate_hardware"], "drained by NHC: GPU missing", category="hardware")

    # NHC resumes nodes it drained itself as soon as its checks pass. After a GPU
    # swap that would put the node straight back into batch with no burn-in.
    CLASSIC(f'sudo scontrol update nodename={node} state=drain reason="triage: hardware: GPU missing, RMA-DRILL-1"')
    r.mark("observe", "drain taken over from NHC (reason no longer starts with NHC:)")
    lab.ssh(node, "sudo systemd-tmpfiles --create /etc/tmpfiles.d/fakegpu.conf")
    r.mark("fix", "GPU replaced (device node restored)")
    time.sleep(NHC_INTERVAL + 20)
    r.check("NHC passed but left the hardware drain alone", "DRAIN" in classic(node), lab.node(CLASSIC, node)["reason"])
    r.decide(node, ["escalate_hardware"], "repaired, not burned in yet")

    CLASSIC(f"sudo scontrol create reservation=drill-burnin nodes={node} users=root starttime=now duration=30 flags=maint,ignore_jobs")
    CLASSIC(f"sudo scontrol update nodename={node} state=resume")
    r.mark("observe", "maintenance reservation on the node, node resumed for burn-in only")
    burn = lab.sbatch(CLASSIC, f"-p burnin --reservation=drill-burnin -w {node} --gres=gpu:4 --exclusive "
                               f"-o /shared/burnin-%j.out -J burnin /usr/local/sbin/lab-burnin", sudo=True)
    user = lab.sbatch(CLASSIC, "-n1 --mem=200M -J drill-user-during-burnin --wrap 'sleep 5'")
    acct = r.wait(f"user job {user} ran", lambda: (a := lab.accounted(user)) and a["state"] == "COMPLETED" and a, 120)
    r.check(f"user work stayed off {node} during burn-in", acct["nodes"] != node, acct)
    acct = r.wait(f"burn-in job {burn} finished", lambda: (a := lab.accounted(burn)) and a["state"] not in ("PENDING", "RUNNING") and a, 300, every=5)
    r.note("burn-in output", output=CLASSIC(f"cat /shared/burnin-{burn}.out", check=False).out.strip().splitlines()[-8:])
    r.check("burn-in passed", acct["state"] == "COMPLETED" and acct["exit"] == "0:0", acct)
    CLASSIC("sudo scontrol delete reservation=drill-burnin")
    r.wait(f"{node} back in batch", lambda: lab.in_service(classic(node)), 60, kind="recover")


def munge_roundtrip(node):
    """The canonical munge test: a credential made on the controller, decoded on the node."""
    cred = CLASSIC("munge -n").out
    res = lab.ssh(node, "unmunge", stdin=cred, check=False)
    text = (res.out + res.err).strip()
    return text.splitlines()[0] if text else f"rc={res.rc}"


@drill("munge-key-mismatch", "A node's munge key no longer matches", minutes=5,
       teaches="Authentication faults look like a dead node to slurmctld. The host is up and slurmd is running; "
               "munge -n | unmunge names the cause. Fixed before SlurmdTimeout, the running job survives.")
def munge_key_mismatch(r):
    """slurm-c2 gets a different munge key (as after a bad reimage or a partial key
    rotation) while it runs a job."""
    node = "slurm-c2"
    restore = ("sudo test -f /etc/munge/munge.key.drill && sudo mv /etc/munge/munge.key.drill /etc/munge/munge.key "
               "&& sudo systemctl restart munge && sudo systemctl restart slurmd; true")
    r.cleanup("restore the munge key", lambda: lab.ssh(node, restore, check=False))
    r.cleanup("cancel drill jobs", lambda: lab.cancel_drill_jobs(CLASSIC))
    job = lab.sbatch(CLASSIC, f"-w {node} -t 10 --mem=200M -J drill-munge-survivor --wrap 'sleep 420'")
    wait_running(r, CLASSIC, job, node, "running job")

    lab.ssh(node, "sudo cp -p /etc/munge/munge.key /etc/munge/munge.key.drill && "
                  "sudo dd if=/dev/urandom of=/etc/munge/munge.key bs=1024 count=1 status=none && sudo systemctl restart munge")
    r.mark("inject", f"{node} has a different munge key")
    r.wait("slurmctld flags it NOT_RESPONDING", lambda: "NOT_RESPONDING" in classic(node), 300, every=3, kind="detect")
    r.decide(node, ["investigate"], "not responding: auth")
    r.note("diagnosis: host reachable, slurmd running", slurmd=lab.ssh(node, "systemctl is-active slurmd", check=False).out.strip())
    r.note("diagnosis: munge -n on slurm-ctl | unmunge on node", result=munge_roundtrip(node))
    r.note("slurmctld log", lines=CLASSIC("sudo grep -iE 'munge|credential' /var/log/slurm/slurmctld.log | tail -3", check=False).out.strip().splitlines())

    lab.ssh(node, restore)
    r.mark("fix", "munge key restored; munge and slurmd restarted")
    r.wait(f"{node} back in service", lambda: lab.in_service(classic(node)), 180, every=2, kind="recover")
    r.check("munge round-trip works again", "Success" in munge_roundtrip(node))
    acct = r.wait(f"job {job} finished", lambda: (a := lab.accounted(job)) and a["state"] not in ("RUNNING", "PENDING") and a, 420, every=10)
    r.check(f"job {job} survived the outage", acct["state"] == "COMPLETED", acct)


@drill("clock-skew", "A node's clock jumps 10 minutes ahead", minutes=5,
       teaches="munge credentials are only valid within ~5 minutes; clock skew is an auth failure in disguise.")
def clock_skew(r):
    """chrony stops on slurm-c1 and its clock jumps 10 minutes ahead."""
    node = "slurm-c1"
    fix = "sudo systemctl start chrony && sleep 3 && sudo chronyc -a makestep >/dev/null && sudo systemctl restart slurmd"
    r.cleanup("restart chrony and slurmd", lambda: lab.ssh(node, fix, check=False))
    r.cleanup(f"resume {node}", lambda: resume_if_out(CLASSIC, node))
    lab.ssh(node, "sudo systemctl stop chrony && sudo date -s '+10 minutes' >/dev/null")
    r.mark("inject", f"chrony stopped on {node}; clock +10 min")
    r.wait("slurmctld flags it NOT_RESPONDING", lambda: "NOT_RESPONDING" in classic(node), 300, every=3, kind="detect")
    r.decide(node, ["investigate"], "not responding: clock")
    skew = int(lab.ssh(node, "date +%s").out) - int(CLASSIC("date +%s").out)
    r.note("diagnosis: clock offset vs slurm-ctl", seconds=skew)
    r.note("diagnosis: munge -n on slurm-ctl | unmunge on node", result=munge_roundtrip(node))
    lab.ssh(node, fix)
    r.mark("fix", "chrony started and stepped the clock; slurmd restarted")
    r.wait(f"{node} back in service", lambda: lab.in_service(classic(node)), 180, every=2, kind="recover")
    skew = int(lab.ssh(node, "date +%s").out) - int(CLASSIC("date +%s").out)
    r.check("clock back in sync", abs(skew) <= 2, {"offset_s": skew})


@drill("slurmctld-outage", "The Slurm controller goes down", minutes=4,
       teaches="Running jobs don't need slurmctld; new work and every Slurm query do. "
               "Triage has to keep working for the clusters it can still see.")
def slurmctld_outage(r):
    """slurmctld stops for a minute while a job runs on slurm-c1."""
    r.cleanup("start slurmctld", lambda: CLASSIC("sudo systemctl start slurmctld", check=False))
    r.cleanup("cancel drill jobs", lambda: lab.cancel_drill_jobs(CLASSIC))
    job = lab.sbatch(CLASSIC, "-w slurm-c1 -t 5 --mem=200M -J drill-ctld-survivor --wrap 'sleep 150'")
    wait_running(r, CLASSIC, job, "slurm-c1", "running job")
    CLASSIC("sudo systemctl stop slurmctld")
    r.mark("inject", "slurmctld stopped")
    res = CLASSIC("squeue", check=False)
    r.mark("detect", "squeue fails", output=(res.err or res.out).strip()[-200:])
    res = CLASSIC("sbatch --wrap true", check=False)
    r.check("new work is refused", res.rc != 0, (res.err or res.out).strip()[-200:])
    r.check("the running job keeps running on its node", lab.ssh("slurm-c1", "pgrep -fx 'sleep 150'", check=False).rc == 0)

    def grade(data):
        """The other clusters are still triaged and the lab cluster is reported unavailable."""
        unavailable = [u.get("cluster") for u in data.get("unavailable", [])]
        return "lab" in unavailable and data["total_nodes"] >= TOTAL_NODES - 2, f"unavailable: {unavailable}"
    r.decide_cluster("controller down", grade)
    time.sleep(30)
    CLASSIC("sudo systemctl start slurmctld")
    r.mark("fix", "slurmctld started")
    r.wait("squeue works again", lambda: CLASSIC("squeue -h", check=False).rc == 0, 120, kind="recover")
    j = lab.job(CLASSIC, job)
    r.check(f"job {job} recovered from saved state, still running", j and "RUNNING" in j["state"], j and sorted(j["state"]))
    acct = r.wait(f"job {job} finished", lambda: (a := lab.accounted(job)) and a["state"] not in ("RUNNING", "PENDING") and a, 240, every=5)
    r.check(f"job {job} completed", acct["state"] == "COMPLETED", acct)


@drill("slurmdbd-outage", "Accounting database daemon goes down", minutes=3,
       teaches="Scheduling continues without slurmdbd; slurmctld queues the records and replays them. "
               "Anything that reads accounting (sacct, triage evidence) goes blind meanwhile.")
def slurmdbd_outage(r):
    """slurmdbd stops; three short jobs run; slurmdbd comes back and the records flush."""
    r.cleanup("start slurmdbd", lambda: CLASSIC("sudo systemctl start slurmdbd", check=False))
    r.cleanup("cancel drill jobs", lambda: lab.cancel_drill_jobs(CLASSIC))
    CLASSIC("sudo systemctl stop slurmdbd")
    r.mark("inject", "slurmdbd stopped")
    res = CLASSIC("sacct -n -X -S now-5minutes", check=False)
    r.mark("detect", "sacct fails", output=(res.err or res.out).strip()[-200:])
    jobs = [lab.sbatch(CLASSIC, f"-n1 --mem=200M -J drill-dbd-{i} --wrap 'sleep 5'") for i in range(3)]
    r.wait("all three jobs ran (scheduling unaffected)", lambda: not any(lab.job(CLASSIC, j) and lab.job(CLASSIC, j)["state"] & {"PENDING", "RUNNING"} for j in jobs), 120)
    r.note("slurmctld's queue of unsent records", sdiag=[ln.strip() for ln in CLASSIC("sdiag").out.splitlines() if "DBD Agent" in ln])

    def grade(data):
        """Every node still triaged from slurmctld's live state; accounting reported as unavailable."""
        return data["total_nodes"] >= TOTAL_NODES, f"{data['total_nodes']} nodes, unavailable: {data.get('unavailable')}"
    r.decide_cluster("accounting down", grade)
    CLASSIC("sudo systemctl start slurmdbd")
    r.mark("fix", "slurmdbd started")
    r.wait("queued records replayed into accounting", lambda: all(lab.accounted(j) and lab.accounted(j)["state"] == "COMPLETED" for j in jobs),
           180, every=3, kind="recover")
    r.note("queue after replay", sdiag=[ln.strip() for ln in CLASSIC("sdiag").out.splitlines() if "DBD Agent" in ln])


# --- Kubernetes and Slinky -------------------------------------------------------------

@drill("slinky-pod-kill", "Slurm worker pod deleted mid-job (vs slurmd killed on a VM)", minutes=6,
       teaches="On a VM, slurmd can die and the job lives on (slurmstepd is separate). "
               "In Slinky the pod is the node: lose the pod and you lose the job.")
def slinky_pod_kill(r):
    """First the classic baseline: slurmd SIGKILLed under a running job. Then the
    Slinky equivalent: the worker pod deleted under a running job."""
    r.cleanup("start slurmd on slurm-c1", lambda: lab.ssh("slurm-c1", "sudo systemctl start slurmd", check=False))
    r.cleanup("cancel classic drill jobs", lambda: lab.cancel_drill_jobs(CLASSIC))
    r.cleanup("cancel Slinky drill jobs", lambda: lab.cancel_drill_jobs(SLINKY))
    r.cleanup("resume slinky-0", lambda: resume_if_out(SLINKY, "slinky-0"))

    cjob = lab.sbatch(CLASSIC, "-w slurm-c1 -t 5 --mem=200M -J drill-classic-baseline --wrap 'sleep 90'")
    wait_running(r, CLASSIC, cjob, "slurm-c1", "classic job")
    lab.ssh("slurm-c1", "sudo systemctl kill -s KILL slurmd")
    r.mark("observe", "classic: slurmd SIGKILLed on slurm-c1")
    time.sleep(3)
    r.check("classic: the job outlives slurmd", lab.ssh("slurm-c1", "pgrep -fx 'sleep 90'", check=False).rc == 0)
    lab.ssh("slurm-c1", "sudo systemctl start slurmd")

    sjob = lab.sbatch(SLINKY, "-w slinky-0 -t 10 --mem=100M -J drill-slinky-victim --wrap 'sleep 600'")
    wait_running(r, SLINKY, sjob, "slinky-0", "Slinky job")
    before = slurm_pod_where("slurm-worker-slinky-0")
    lab.kubectl("-n slurm delete pod slurm-worker-slinky-0 --wait=false")
    r.mark("inject", "Slinky: worker pod slurm-worker-slinky-0 deleted under a running job",
           pod_was_on=before and before["node"])
    r.wait("Slurm notices", lambda: not lab.in_service(slinky_state("slinky-0")) or not lab.running_on(SLINKY, sjob, "slinky-0"),
           120, every=1, kind="detect")
    r.note("Slinky's view", node=sorted(slinky_state("slinky-0")), job=(lambda j: j and sorted(j["state"]))(lab.job(SLINKY, sjob)))
    # wait while the replacement registers, resume once it has (it takes seconds)
    r.decide("slinky-0", ["wait", "resume"], "worker pod gone")
    r.wait("operator recreated the worker pod", lambda: (p := slurm_pod_where("slurm-worker-slinky-0")) and p["ready"] and not p["terminating"], 180)
    back = r.wait("slinky-0 back in service by itself", lambda: lab.in_service(slinky_state("slinky-0")), 120, every=2, required=False)
    if not back:
        r.decide("slinky-0", ["resume"], "pod back, node still out")
        SLINKY("scontrol update nodename=slinky-0 state=resume", check=False)
        r.mark("fix", "slinky-0 resumed by hand (Slinky runs ReturnToService=0)")
    r.wait("slinky-0 in service", lambda: lab.in_service(slinky_state("slinky-0")), 120, kind="recover")
    j = lab.job(SLINKY, sjob)
    r.note("what happened to the Slinky job", state=j and sorted(j["state"]), restarts=j and j["restarts"], reason=j and j["reason"])
    acct = r.wait(f"classic job {cjob} finished", lambda: (a := lab.accounted(cjob)) and a["state"] not in ("RUNNING", "PENDING") and a, 120, every=5)
    r.check("classic: job completed despite the slurmd kill", acct["state"] == "COMPLETED", acct)


@drill("k8s-pdb-drain", "Drain a Kubernetes worker under PDBs, with a busy Slurm pod on it", minutes=9,
       teaches="Drains stop at PodDisruptionBudgets, including the one Slinky puts on workers running jobs. "
               "Node-local volumes pin pods: what lived on the drained node waits for it.")
def k8s_pdb_drain(r):
    """k8s-w1 is drained for maintenance. A 2-replica service with minAvailable=2
    blocks it, and Slinky protects slinky-0 while its job runs."""
    W = "k8s-w1"
    r.cleanup("delete the drill namespace", delete_drill_ns)
    r.cleanup(f"uncordon {W}", lambda: lab.kubectl(f"uncordon {W}", check=False))
    r.cleanup("cancel Slinky drill jobs", lambda: lab.cancel_drill_jobs(SLINKY))
    drill_deployment("checkout-api", 2, pdb_min_available=2)
    r.note("checkout-api placement", pods=placement("checkout-api"))
    sjob = lab.sbatch(SLINKY, "-w slinky-0 -t 4 --mem=100M -J drill-slinky-busy --wrap 'sleep 150'")
    wait_running(r, SLINKY, sjob, "slinky-0", "Slinky job")
    r.wait("operator protects the busy worker pod", lambda: (slurm_pod_where("slurm-worker-slinky-0") or {}).get("labels", {}).get(
        "nodeset.slinky.slurm.net/pod-protect") == "true", 60)

    lab.kubectl(f'annotate node {W} --overwrite "slurm-k8s-lab/triage-reason=maint: kernel update CHG-3002"')
    r.cleanup("remove the maintenance reason", lambda: lab.kubectl(f"annotate node {W} slurm-k8s-lab/triage-reason-", check=False))
    r.mark("inject", f"kubectl drain {W} for maintenance (45 s timeout)")
    res = lab.kubectl(f"drain {W} --ignore-daemonsets --delete-emptydir-data --timeout=45s", check=False, timeout=90)
    blocked = sorted({f"{m[1]}/{m[0]}" for m in re.findall(r'evicting pods?/"([^"]+)" -n "([^"]+)".*disruption budget', res.out + res.err)})
    r.mark("detect", "drain blocked by disruption budgets", pods=blocked)
    r.check("drain did not complete", res.rc != 0)
    r.decide(W, ["investigate"], "cordoned, drain blocked")
    r.decide("slinky-0", ["wait"], "operator-drained, job running")

    lab.kubectl("-n drill scale deploy/checkout-api --replicas=3")
    lab.kubectl("-n drill rollout status deploy/checkout-api --timeout=120s", timeout=150)
    r.mark("fix", "checkout-api scaled to 3 so one replica can move; draining again (waits for the Slurm job)")
    res = lab.kubectl(f"drain {W} --ignore-daemonsets --delete-emptydir-data --timeout=300s", check=False, timeout=360)
    r.check("drain completed", res.rc == 0, (res.err or "").strip().splitlines()[-3:])
    r.mark("observe", f"{W} empty")
    acct_job = lab.job(SLINKY, sjob)
    r.note("Slinky job when the drain finished", state=acct_job and sorted(acct_job["state"]))
    r.note("where things went", checkout=placement("checkout-api"),
           slinky0=(slurm_pod_where("slurm-worker-slinky-0") or {}).get("node"),
           pending=[f"{p['namespace']}/{p['name']}" for p in lab.pods() if p["phase"] == "Pending"])
    r.decide(W, ["resume"], "cordoned for maintenance, empty")

    lab.kubectl(f"uncordon {W}")
    lab.kubectl(f"annotate node {W} slurm-k8s-lab/triage-reason-", check=False)
    r.mark("observe", f"maintenance done; {W} uncordoned")
    r.wait("nothing Pending any more", lambda: not [p for p in lab.pods() if p["phase"] == "Pending"], 300, every=5)
    r.wait("slinky-0 in service", lambda: lab.in_service(slinky_state("slinky-0")), 180, every=3, kind="recover")
    r.note("after uncordon (no automatic rebalancing)", checkout=placement("checkout-api"),
           slinky0=(slurm_pod_where("slurm-worker-slinky-0") or {}).get("node"))


def kubelet_stop(r, W):
    """Stop the kubelet on worker W (its containers keep running) with half of a
    drill service and a Slinky job on it; follow both schedulers through the
    eviction and back."""
    other = "k8s-w1" if W == "k8s-w2" else "k8s-w2"
    r.cleanup(f"start the kubelet on {W}", lambda: lab.ssh(W, "sudo systemctl start kubelet", check=False))
    r.cleanup("delete the drill namespace", delete_drill_ns)
    r.cleanup("cancel Slinky drill jobs", lambda: lab.cancel_drill_jobs(SLINKY))
    drill_deployment("web", 4)
    r.note("web placement", pods=placement("web"))
    slurm_pods = {p["name"]: p["node"] for p in lab.pods(namespace="slurm")}
    r.note("Slinky placement", pods=slurm_pods)
    hosts_control_plane = any(node == W for name, node in slurm_pods.items() if "controller" in name or "restapi" in name)
    sl_node = next(n for n in ("slinky-0", "slinky-1") if slurm_pods.get(f"slurm-worker-{n}") == W)
    sjob = lab.sbatch(SLINKY, f"-w {sl_node} -t 20 --mem=100M -J drill-slinky-on-{W} --wrap 'sleep 900'")
    wait_running(r, SLINKY, sjob, sl_node, "Slinky job")

    lab.ssh(W, "sudo systemctl stop kubelet")
    r.mark("inject", f"kubelet stopped on {W} (containers keep running)")
    r.wait(f"{W} Ready=Unknown", lambda: lab.k8s_node(W)["conditions"].get("Ready") == "Unknown", 120, every=2, kind="detect")
    r.decide(W, ["investigate"], "kubelet silent, eviction countdown")
    r.note("Slurm's view at the same moment", node=safe(lambda: sorted(slinky_state(sl_node))),
           job=safe(lambda: sorted(lab.job(SLINKY, sjob)["state"])))
    if hosts_control_plane:
        # slurmrestd's pod is marked NotReady and leaves its Service: triage can't
        # see Slinky at all, so the answer has to come at cluster level.
        def grade(data):
            """The kubelet-less node is flagged, and the Slinky outage is traced to it."""
            recs = {x["node"]: x for x in data["recommendations"]}
            restd = recs.get("slurmrestd", {})
            ok = recs.get(W, {}).get("action") == "investigate" and W in restd.get("summary", "")
            return ok, f"{W}: {recs.get(W, {}).get('action')}; slurmrestd: {restd.get('summary', 'not reported')}"
        r.decide_cluster("Slinky's control plane on the silent node", grade)
    else:
        r.decide(sl_node, ["investigate"], "Slurm node on an unreachable Kubernetes node", category="kubernetes_node")
    r.wait("pods on the node marked for eviction (300 s toleration)", lambda: "terminating" in placement("web").get(W, []), 420, every=5)
    r.wait(f"replacement web pods running on {other}", lambda: placement("web").get(other, []).count("Running") >= 4, 120, every=3)
    r.note("Slinky after the eviction",
           pods={p["name"]: ("terminating" if p["terminating"] else p["phase"]) + "@" + p["node"] for p in lab.pods(namespace="slurm")},
           node=safe(lambda: sorted(slinky_state(sl_node))), job=safe(lambda: sorted(lab.job(SLINKY, sjob)["state"])))
    r.decide_cluster("after the eviction", cluster_ok if not hosts_control_plane else lambda d: (
        any(x["node"] == "slurmrestd" and W in x["summary"] for x in d["recommendations"]), "slurmrestd traced to " + W))

    lab.ssh(W, "sudo systemctl start kubelet")
    r.mark("fix", f"kubelet started on {W}")
    r.wait(f"{W} Ready", lambda: lab.k8s_node(W)["conditions"].get("Ready") == "True", 120, every=2, kind="recover")
    r.wait("Slinky whole again", lambda: all(lab.in_service(slinky_state(n)) for n in ("slinky-0", "slinky-1")), 300, every=5)
    j = safe(lambda: lab.job(SLINKY, sjob))
    r.note("Slinky job fate", job=j if not isinstance(j, dict) else {"state": sorted(j["state"]), "reason": j["reason"]})
    r.note("web after recovery (no rebalancing)", pods=placement("web"))


@drill("k8s-kubelet-stop", "kubelet dies on the worker hosting Slinky's controller", minutes=11,
       teaches="Kubernetes and Slurm disagree: K8s declares the node lost and evicts after 5 minutes while slurmd "
               "keeps running jobs. Pods on the node drop out of their Services at once, so Slinky's slurmrestd "
               "vanishes with its container still running.")
def k8s_kubelet_stop(r):
    """The kubelet stops on k8s-w2, which hosts slurm-controller-0, slurmrestd and
    slinky-1, plus half of a drill service."""
    kubelet_stop(r, "k8s-w2")


@drill("k8s-kubelet-stop-worker", "kubelet dies on a plain Slurm worker node", minutes=11,
       teaches="With Slinky's control plane elsewhere, Slurm keeps seeing the worker as fine while Kubernetes "
               "counts down to evicting it: triage has to join the two views.")
def k8s_kubelet_stop_worker(r):
    """The kubelet stops on k8s-w1, which hosts slinky-0 (and Prometheus) but not
    Slinky's control plane."""
    kubelet_stop(r, "k8s-w1")


@drill("k8s-cert-renewal", "Renew the control-plane certificates", minutes=5,
       teaches="kube-apiserver picks up a renewed serving certificate by itself, but the control-plane components "
               "only read their kubeconfig client certificates at startup, so they still need a restart: on one "
               "control-plane node that's an API outage, so time it.")
def k8s_cert_renewal(r):
    """kubeadm renews every control-plane certificate; the control-plane static
    pods are restarted to pick them up while an off-node probe times the API outage."""
    restore = "sudo sh -c 'mv /etc/kubernetes/manifests-drill/*.yaml /etc/kubernetes/manifests/ 2>/dev/null; true'"
    r.cleanup("put the static pod manifests back", lambda: lab.ssh("k8s-cp", restore, check=False))
    enddate = "sudo openssl x509 -enddate -noout -in /etc/kubernetes/pki/apiserver.crt"
    before = lab.ssh("k8s-cp", enddate).out.strip()
    r.note("apiserver certificate before", notAfter=before)
    # Probe from a worker: one request a second for 4 minutes.
    probe = subprocess.Popen(lab.ssh_argv("k8s-w1", "for i in $(seq 240); do printf '%s %s\\n' $(date +%s) "
                                                     "$(curl -sk -m1 -o /dev/null -w '%{http_code}' https://10.0.5.133:6443/readyz); sleep 1; done"),
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL, text=True)
    r.cleanup("stop the probe", probe.kill)
    time.sleep(5)
    lab.ssh("k8s-cp", "sudo kubeadm certs renew all >/dev/null")
    r.mark("inject", "kubeadm certs renew all")
    after_renew = lab.ssh("k8s-cp", "sudo openssl x509 -enddate -noout -in /etc/kubernetes/pki/apiserver.crt").out.strip()
    served = lab.ssh("k8s-w1", "echo | openssl s_client -connect 10.0.5.133:6443 2>/dev/null | openssl x509 -noout -enddate").out.strip()
    r.check("certificates renewed on disk", after_renew != before, {"before": before, "after": after_renew})
    # kube-apiserver reloads its serving certificate by itself; what the restart is
    # for is the client certificates inside the kubeconfigs (controller-manager,
    # scheduler) that the components read only at startup.
    r.note("serving certificate the API presents before any restart", served=served,
           already_renewed=served == after_renew)
    lab.ssh("k8s-cp", "sudo mkdir -p /etc/kubernetes/manifests-drill && sudo sh -c 'mv /etc/kubernetes/manifests/*.yaml /etc/kubernetes/manifests-drill/'")
    r.wait("control-plane processes stopped", lambda: lab.ssh("k8s-cp", "pgrep -x 'kube-apiserver|etcd'", check=False).rc == 1, 120, every=2)
    lab.ssh("k8s-cp", restore)
    r.mark("fix", "static pod manifests back; kubelet restarts the control plane")
    r.wait("API ready again", lambda: lab.kubectl("get --raw /readyz", check=False).out.strip() == "ok", 300, every=2, kind="recover")
    served = lab.ssh("k8s-w1", "echo | openssl s_client -connect 10.0.5.133:6443 2>/dev/null | openssl x509 -noout -enddate").out.strip()
    r.check("the API serves the renewed certificate", served == after_renew, served)
    lab.ssh("k8s-cp", "sudo cp /etc/kubernetes/admin.conf $HOME/.kube/config && sudo chown $(id -u):$(id -g) $HOME/.kube/config")
    r.check("kubectl works with the renewed admin credentials", lab.kubectl("get nodes", check=False).rc == 0)
    time.sleep(20)
    probe.kill()
    samples = [ln.split() for ln in (probe.stdout.read() or "").splitlines() if len(ln.split()) == 2]
    down = [int(t) for t, code in samples if code != "200"]
    r.note("API outage seen from k8s-w1", seconds_not_200=len(down),
           window=f"{min(down) - int(samples[0][0])}s..{max(down) - int(samples[0][0])}s after probe start" if down else "none")
    r.decide_cluster("after renewal", cluster_ok)


# --- node lifecycle ------------------------------------------------------------------------

def c3_tfvars(r, stage):
    path = r.log.with_name(r.log.stem + f"_c3-{stage}.tfvars.json")
    path.write_text(json.dumps({"extra_compute_nodes": {"slurm-c3": {"vmid": 136, "ip": "10.0.5.136", "stage": stage}}}))
    return path


@drill("node-bringup", "New compute node: provision, burn in, promote", minutes=16, needs=("bringup", "nhc"),
       teaches="A node joins in a burn-in stage, outside production. It earns its way into batch by passing "
               "burn-in, and promotion is a one-line change in code.")
def node_bringup(r):
    """slurm-c3 is added in code with stage=burnin. Terraform builds it, Ansible
    configures it into the burnin partition only, a burn-in job qualifies it, and
    stage=production promotes it into batch. Cleanup removes it again."""
    node = "slurm-c3"

    def remove():
        CLASSIC(f"sudo scontrol update nodename={node} state=drain reason='drill: removing'", check=False)
        lab.terraform_apply(log=r.log)
        lab.ansible("site.yml", limit="slurm", log=r.log)
        subprocess.run(["ssh-keygen", "-R", lab.HOSTS[node]], capture_output=True)
    r.cleanup(f"remove {node}", remove)
    subprocess.run(["ssh-keygen", "-R", lab.HOSTS[node]], capture_output=True)

    lab.terraform_apply(c3_tfvars(r, "burnin"), log=r.log)
    r.mark("inject", f"{node} added to the node table (stage=burnin); terraform apply done")
    r.wait(f"{node} reachable over SSH", lambda: lab.ssh(node, "cloud-init status --wait >/dev/null; true", check=False, timeout=300).rc == 0, 300, every=5)
    recap = lab.ansible("site.yml", limit="slurm", log=r.log)
    r.mark("observe", "configured by Ansible (site.yml --limit slurm)", recap=recap)
    n = r.wait(f"{node} registered, idle, in burnin only", lambda: (x := lab.node(CLASSIC, node)) and lab.in_service(x["state"]) and x["partitions"] == ["burnin"] and x,
               180, every=3, kind="detect")
    r.decide(node, ["burn_in"], "new node, not qualified")
    res = CLASSIC(f"sbatch -p batch -w {node} --mem=200M -J drill-sneak --wrap true", check=False)
    r.check(f"production work can't reach {node}", res.rc != 0, (res.err or res.out).strip()[-160:])

    burn = lab.sbatch(CLASSIC, f"-p burnin -w {node} --gres=gpu:4 --exclusive -o /shared/burnin-%j.out -J burnin /usr/local/sbin/lab-burnin", sudo=True)
    acct = r.wait(f"burn-in job {burn} finished", lambda: (a := lab.accounted(burn)) and a["state"] not in ("PENDING", "RUNNING") and a, 300, every=5)
    r.note("burn-in output", output=CLASSIC(f"cat /shared/burnin-{burn}.out", check=False).out.strip().splitlines()[-8:])
    r.check("burn-in passed", acct["state"] == "COMPLETED" and acct["exit"] == "0:0", acct)
    r.decide(node, ["promote"], "burn-in passed")

    lab.terraform_apply(c3_tfvars(r, "production"), log=r.log)
    recap = lab.ansible("site.yml", limit="slurm", log=r.log)
    r.mark("fix", f"{node} promoted in code (stage=production); terraform + Ansible applied", recap=recap)
    r.wait(f"{node} in batch", lambda: "batch" in lab.node(CLASSIC, node)["partitions"], 120, every=3)
    first = lab.sbatch(CLASSIC, f"-p batch -w {node} --mem=200M -J drill-first-prod --wrap hostname")
    r.wait(f"first production job ran on {node}", lambda: (a := lab.accounted(first)) and a["state"] == "COMPLETED", 120, every=3, kind="recover")
    r.decide(node, ["none"], "in production")
