#!/usr/bin/env python3
"""Make a raw demo recording watchable without changing what's on screen.

run_demo.py records real wall-clock timings. Model answers then arrive as one
burst of ~30 lines followed by a 1.5 s pause, which nobody can read, and model
latency shows up as dead air. This rewrites only the timestamps:

  - any gap longer than --max-gap is shortened to --max-gap (like asciinema's
    idle_time_limit), and
  - after a burst of output, the next gap is stretched to a reading pause of
    --per-line seconds per line, capped at --max-hold.

The output text and its order are untouched.

  python demo/retime.py demo/node-triage-demo.raw.cast demo/node-triage-demo.cast
"""

import argparse
import json


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("dst")
    ap.add_argument("--max-gap", type=float, default=1.5)
    ap.add_argument("--per-line", type=float, default=0.3)
    ap.add_argument("--max-hold", type=float, default=12.0)
    ap.add_argument("--burst-lines", type=int, default=6, help="lines of output that count as a burst worth reading")
    ap.add_argument("--height", type=int, default=None, help="override terminal rows in the header")
    args = ap.parse_args()

    with open(args.src, encoding="utf-8") as f:
        header = json.loads(f.readline())
        events = [json.loads(line) for line in f if line.strip()]

    header.pop("idle_time_limit", None)  # timing is now explicit
    if args.height:
        header["height"] = args.height

    out, clock, prev_t, lines_since_gap = [], 0.0, 0.0, 0
    for t, kind, data in events:
        gap = t - prev_t
        if gap > 0.8:  # a pause point: model latency, or the recorder's own pause
            hold = args.per_line * lines_since_gap if lines_since_gap >= args.burst_lines else 0.0
            gap = max(min(gap, args.max_gap), min(hold, args.max_hold))
            lines_since_gap = 0
        clock += gap
        prev_t = t
        if kind == "o":
            lines_since_gap += data.count("\n")
        out.append([round(clock, 3), kind, data])
    # Hold the final frame so the last sinfo is readable before the GIF loops.
    out.append([round(clock + 4.0, 3), "o", ""])

    with open(args.dst, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(header) + "\n")
        for e in out:
            f.write(json.dumps(e) + "\n")
    print(f"{len(events)} events: {events[-1][0]:.1f}s raw -> {out[-1][0]:.1f}s presented")


if __name__ == "__main__":
    main()
