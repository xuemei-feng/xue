#!/usr/bin/env python3
"""
Plot iperf3 -J logs: end sender throughput vs run time (from filename timestamp).

  sudo apt-get install -y python3-matplotlib

  PNG output (first match wins):
  - -o /path/out.png           full path
  - -d /some/dir               writes DIR/iperf3_throughput.png
  - --cwd                      pwd/iperf3_throughput.png
  - env IPERF3_PNG_DIR=/path   same as -d
  - else                       <this script’s directory>/iperf3_throughput.png  (e.g. …/xue/scripts/)

  Cursor cannot pass “active file’s folder” into a plain terminal. Use the Task
  “iperf3: plot JSON → PNG beside active editor file” (uses ${fileDirname}), or run:
    python3 scripts/plot_iperf3_json.py ~/iperf_logs_json -d /path/to/same/folder/as/open/file
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from datetime import datetime

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def main() -> int:
    ap = argparse.ArgumentParser(description="Plot iperf3 result_*.json throughput over time.")
    ap.add_argument(
        "logdir",
        nargs="?",
        default=os.path.expanduser("~/iperf_logs_json"),
        help="Directory containing result_YYYYMMDD_HHMMSS.json",
    )
    ap.add_argument(
        "-o",
        "--output",
        default="",
        help="Output PNG file path (overrides -d, --cwd, env, and default scripts/ dir)",
    )
    ap.add_argument(
        "-d",
        "--output-dir",
        dest="output_dir",
        default="",
        metavar="DIR",
        help="Directory for iperf3_throughput.png (use Cursor Task with ${fileDirname})",
    )
    ap.add_argument(
        "--cwd",
        action="store_true",
        help="Write PNG under the shell's current working directory (pwd), not scripts/",
    )
    args = ap.parse_args()

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        print("Need matplotlib:  sudo apt-get install -y python3-matplotlib", file=sys.stderr)
        return 1

    pattern = os.path.join(args.logdir, "result_*.json")
    files = sorted(glob.glob(pattern))
    times: list[datetime] = []
    mbps_list: list[float] = []

    for path in files:
        base = os.path.basename(path)
        if not base.startswith("result_") or not base.endswith(".json"):
            continue
        stamp = base[len("result_") : -len(".json")]
        try:
            t = datetime.strptime(stamp, "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            print(f"skip {path}: {e}", file=sys.stderr)
            continue
        bps = data.get("end", {}).get("sum_sent", {}).get("bits_per_second")
        if bps is None:
            print(f"skip {path}: no end.sum_sent.bits_per_second", file=sys.stderr)
            continue
        times.append(t)
        mbps_list.append(float(bps) / 1e6)

    if not times:
        print(f"No usable JSON in {args.logdir}", file=sys.stderr)
        return 1

    if args.output:
        out = args.output
    elif args.output_dir and args.output_dir.strip():
        out = os.path.join(os.path.expanduser(args.output_dir.strip()), "iperf3_throughput.png")
    elif args.cwd:
        out = os.path.join(os.getcwd(), "iperf3_throughput.png")
    else:
        env_dir = os.environ.get("IPERF3_PNG_DIR", "").strip()
        if env_dir:
            out = os.path.join(os.path.expanduser(env_dir), "iperf3_throughput.png")
        else:
            out = os.path.join(_SCRIPT_DIR, "iperf3_throughput.png")

    out = os.path.abspath(os.path.expanduser(out))
    parent = os.path.dirname(out)
    if parent and not os.path.isdir(parent):
        print(f"Output directory does not exist: {parent}", file=sys.stderr)
        return 1

    avg = sum(mbps_list) / len(mbps_list)
    ymin, ymax = min(mbps_list), max(mbps_list)

    fig, ax = plt.subplots(figsize=(11, 4.2), dpi=120)
    ax.plot(times, mbps_list, marker="o", linewidth=1.8, markersize=7, color="#1a5fb4", label="Sender (end avg)")
    ax.axhline(avg, color="#c64600", linestyle="--", linewidth=1.2, label=f"Mean {avg:.2f} Mbps")
    ax.fill_between(times, ymin, ymax, alpha=0.12, color="#1a5fb4", label=f"Range [{ymin:.2f}, {ymax:.2f}] Mbps")

    ax.set_ylabel("Throughput (Mbps)")
    ax.set_xlabel("Run start time (from filename)")
    ax.set_title("iperf3 TCP — per-run sender throughput")
    ax.grid(True, alpha=0.35)
    ax.legend(loc="best", fontsize=9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    fig.autofmt_xdate(rotation=22)

    plt.tight_layout()
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)
    if not os.path.isfile(out) or os.path.getsize(out) <= 0:
        print(f"ERROR: PNG not created or empty at {out}", file=sys.stderr)
        return 1
    sz = os.path.getsize(out)
    print(
        f"Wrote {out}  ({sz} bytes)  n={len(times)} avg={avg:.2f} Mbps span={ymax - ymin:.2f} Mbps",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
