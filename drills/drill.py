#!/usr/bin/env python3
"""Failure drills for the slurm-k8s-lab.

Each drill breaks one thing on purpose and keeps a timeline:

  inject   the fault (or the planned change) goes in
  detect   the scheduler notices
  decide   node-triage is asked what to do, over MCP, read-only, the way an
           on-call agent asks it; the answer is checked against what a good
           engineer would do
  fix      the remedy goes in
  recover  the node or service is back

then undoes everything it did and waits until the whole lab is green again,
so drills can run back to back. Run from the control node (the same place
as scripts/rebuild.sh), with terraform and ansible-playbook on PATH:

  drills/drill.py list
  drills/drill.py check               # is the lab green right now?
  drills/drill.py run node-death      # one or more drills, or --all
  drills/drill.py report              # rebuild drills/SCORECARD.md from results/

A drill ends PASS (everything as expected), GAP (the lab recovered, but
node-triage's decision wasn't the right one: a finding to fix), or FAIL
(the drill itself didn't go as designed, or the lab didn't get back to green).
"""

import argparse
import sys

import lab
from engine import REGISTRY, report, run_drill
import catalog  # noqa: F401  (registers the drills)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    sub.add_parser("check")
    sub.add_parser("report")
    rp = sub.add_parser("run")
    rp.add_argument("names", nargs="*")
    rp.add_argument("--all", action="store_true")
    rp.add_argument("--force", action="store_true", help="run even if the lab isn't green first")
    rp.add_argument("--capture", action="store_true", help="save triage fixtures at every decision point")
    args = ap.parse_args()

    if args.cmd == "list":
        for d in REGISTRY.values():
            needs = f" [needs {', '.join(d.needs)}]" if d.needs else ""
            print(f"{d.name:22} ~{d.minutes:>2} min  {d.title}{needs}")
        return 0
    if args.cmd == "check":
        probs = lab.lab_problems()
        print("green" if not probs else "NOT green:\n  " + "\n  ".join(probs))
        return 0 if not probs else 1
    if args.cmd == "report":
        report()
        return 0
    names = list(REGISTRY) if args.all else args.names
    unknown = [n for n in names if n not in REGISTRY]
    if unknown or not names:
        print(f"unknown or no drills: {unknown}; see 'list'", file=sys.stderr)
        return 2
    results = [run_drill(REGISTRY[n], args) for n in names]
    print("\n==> summary")
    for n, r in zip(names, results):
        print(f"  {n:22} {r['status'] if r else 'SKIPPED'}")
    return 0 if all(r and r["status"] != "FAIL" for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
