#!/usr/bin/env python3
"""Plot normalized latency scaling from wal-bench log files.

Usage:
    python plot_latency.py fdatasync_latency.logs
    python plot_latency.py fdatasync_latency.logs dsync_latency.logs
"""

import re
import sys
import os

import matplotlib.pyplot as plt


def parse_logs(path):
    """Parse a wal-bench latency log file into {concurrency: {percentile: value}}."""
    data = {}
    current_c = None
    with open(path) as f:
        for line in f:
            m = re.match(r"^(\d+):", line)
            if m:
                current_c = int(m.group(1))
                data[current_c] = {}
                continue
            m = re.match(r"^\s+(min|p50|p90|p99|p99\.9|max):\s+(\d+)", line)
            if m and current_c is not None:
                data[current_c][m.group(1)] = int(m.group(2))
    return data


def plot_one(ax, data, label_prefix=""):
    """Plot normalized percentiles for one dataset."""
    concurrency = sorted(data.keys())
    percentiles = ["p50", "p90", "p99", "p99.9"]
    markers = ["o", "s", "^", "D"]

    for pct, marker in zip(percentiles, markers):
        values = [data[c][pct] for c in concurrency]
        base = values[0]
        normed = [v / base for v in values]
        label = f"{label_prefix}{pct}" if label_prefix else pct
        ax.plot(concurrency, normed, f"{marker}-", label=label, linewidth=2)

    return concurrency


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <logs_file> [<logs_file> ...]", file=sys.stderr)
        sys.exit(1)

    log_files = sys.argv[1:]
    single = len(log_files) == 1

    fig, ax = plt.subplots(figsize=(8, 5))
    all_concurrency = []

    for path in log_files:
        data = parse_logs(path)
        name = os.path.splitext(os.path.basename(path))[0].replace("_latency", "").replace("_", " ")
        prefix = "" if single else f"{name} "
        c = plot_one(ax, data, label_prefix=prefix)
        all_concurrency = c if len(c) > len(all_concurrency) else all_concurrency

    ax.plot(all_concurrency, all_concurrency, "k--", label="y = x (linear)", linewidth=1, alpha=0.5)

    ax.set_xlabel("Concurrency")
    ax.set_ylabel("Normalized latency (x value at c=1)")
    if single:
        name = os.path.splitext(os.path.basename(log_files[0]))[0].replace("_latency", "").replace("_", " ")
        ax.set_title(f"{name} latency scaling (normalized, local SSD, 512B O_DIRECT)")
    else:
        ax.set_title("Latency scaling comparison (normalized, local SSD, 512B O_DIRECT)")
    ax.set_xticks(all_concurrency)
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    fig.tight_layout()

    # Output PNG: derive from first log file name
    base = os.path.splitext(log_files[0])[0]
    if not single:
        base = os.path.join(os.path.dirname(log_files[0]), "comparison")
    out = base + ".png"
    fig.savefig(out, dpi=150)
    print(f"Saved to {out}")


if __name__ == "__main__":
    main()
