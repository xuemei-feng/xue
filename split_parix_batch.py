#!/usr/bin/env python3
"""Split a Parix batch log so each worker handles disjoint stripe_id sets."""

import os
import sys


def main() -> int:
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} INPUT.log NUM_CLIENTS OUT_PREFIX", file=sys.stderr)
        print("  OUT_PREFIX0.log .. OUT_PREFIX{N-1}.log", file=sys.stderr)
        print("  Assignment: stripe_id % NUM_CLIENTS == worker_id", file=sys.stderr)
        return 1

    input_path = sys.argv[1]
    num_clients = int(sys.argv[2])
    out_prefix = sys.argv[3]
    if num_clients <= 0:
        print("NUM_CLIENTS must be > 0", file=sys.stderr)
        return 1

    out_dir = os.path.dirname(os.path.abspath(out_prefix))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    outs = []
    for i in range(num_clients):
        path = f"{out_prefix}{i}.log"
        f = open(path, "w", encoding="utf-8")
        f.write(f"# worker {i}/{num_clients} from {input_path}\n")
        f.write(f"# rule: stripe_id % {num_clients} == {i}\n")
        outs.append(f)

    counts = [0] * num_clients
    with open(input_path, "r", encoding="utf-8") as inp:
        for line in inp:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            parts = stripped.split()
            stripe_id = int(parts[0])
            worker = stripe_id % num_clients
            outs[worker].write(line if line.endswith("\n") else line + "\n")
            counts[worker] += 1

    for f in outs:
        f.close()

    total = sum(counts)
    print(f"split {input_path} -> {num_clients} files prefix={out_prefix}")
    for i, c in enumerate(counts):
        print(f"  worker {i}: {out_prefix}{i}.log lines={c}")
    print(f"total update lines={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
