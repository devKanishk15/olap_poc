#!/usr/bin/env python3
"""
analyse_results.py — Aggregate JSONL results into summary tables and charts.

Reads all .jsonl files from /opt1/olap_poc/poc/results/, produces:
  - 01_raw_results.csv
  - 02_summary_table.md
  - 03_charts/latency_*.png
  - 03_charts/write_throughput.png
  - 03_charts/memory_*.png

Usage:
    python report/analyse_results.py [--results /opt1/olap_poc/poc/results] [--out /opt1/olap_poc/poc/report]
"""

import argparse
import json
import os
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from tabulate import tabulate

ENGINES = ["doris", "duckdb", "clickhouse"]
MODES   = ["local", "gcs"]
COLORS  = {"doris": "#1a73e8", "duckdb": "#fbbc04", "clickhouse": "#e53935"}


def load_results(results_dir: Path) -> pd.DataFrame:
    records = []
    for f in sorted(results_dir.glob("*.jsonl")):
        for line in f.read_text().splitlines():
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return pd.DataFrame(records)


def build_read_summary(df: pd.DataFrame) -> pd.DataFrame:
    read = df[df["status"] == "OK"].copy()
    read = read[read.get("query_id", pd.Series(dtype=str)).str.match(r"Q\d+", na=False)]
    if read.empty:
        return pd.DataFrame()

    rows = []
    for (qid, engine, mode), grp in read.groupby(["query_id", "engine", "mode"]):
        row = grp.iloc[0]
        rows.append({
            "query_id":      qid,
            "engine":        engine,
            "mode":          mode,
            "cold_s":        row.get("cold_s"),
            "warm_median_s": row.get("warm_median_s"),
            "warm_p95_s":    row.get("warm_p95_s"),
            "cold_vs_warm":  row.get("cold_vs_warm"),
            "rows_returned": row.get("rows_returned"),
            "spill":         row.get("spill", False),
        })
    return pd.DataFrame(rows).sort_values(["mode", "query_id", "engine"])


def build_write_summary(df: pd.DataFrame) -> pd.DataFrame:
    write_cols = ["workload", "engine", "status", "rows_per_s",
                  "elapsed_s", "total_s", "median_batch_ms", "p95_batch_ms",
                  "rows_affected", "semantic_note"]
    write = df[df.get("workload", pd.Series(dtype=str)).str.match(r"W\d+", na=False)].copy()
    if write.empty:
        return pd.DataFrame()
    cols = [c for c in write_cols if c in write.columns]
    return write[cols].sort_values(["workload", "engine"])


def write_csv(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False)
    print(f"  Wrote: {path}")


def write_summary_md(read_df: pd.DataFrame, write_df: pd.DataFrame, path: Path):
    lines = [
        "# OLAP POC — Benchmark Summary\n",
        f"_Generated: {datetime.utcnow().isoformat()}Z_\n",
        "---\n",
        "## Read Benchmarks — Local Mode\n",
    ]

    for mode in MODES:
        sub = read_df[read_df["mode"] == mode] if not read_df.empty else pd.DataFrame()
        lines.append(f"### Mode: `{mode}`\n")
        if sub.empty:
            lines.append("_No results yet._\n")
            continue

        pivot = sub.pivot_table(
            index="query_id",
            columns="engine",
            values=["warm_median_s", "warm_p95_s", "spill"],
            aggfunc="first"
        )
        table_rows = []
        for qid in sorted(pivot.index):
            row = [qid]
            for eng in ENGINES:
                try:
                    med   = pivot.get(("warm_median_s", eng), {}).get(qid)
                    p95   = pivot.get(("warm_p95_s",    eng), {}).get(qid)
                    spill = pivot.get(("spill",         eng), {}).get(qid)
                    cell  = f"{med:.3f}s / {p95:.3f}s{'  🌊' if spill else ''}" if med else "—"
                except Exception:
                    cell = "—"
                row.append(cell)
            table_rows.append(row)

        headers = ["Query", "Doris (med/p95)", "DuckDB (med/p95)", "ClickHouse (med/p95)"]
        lines.append(tabulate(table_rows, headers=headers, tablefmt="github"))
        lines.append("\n🌊 = spill to disk observed\n")

    lines += [
        "---\n",
        "## Write Benchmarks\n",
    ]
    if not write_df.empty:
        for wid in sorted(write_df["workload"].unique()):
            lines.append(f"\n### `{wid}`\n")
            sub = write_df[write_df["workload"] == wid]
            table_rows = []
            for _, r in sub.iterrows():
                table_rows.append([
                    r.get("engine", "—"),
                    r.get("status", "—"),
                    f"{r.get('rows_per_s', 0):,.0f}" if r.get("rows_per_s") else "—",
                    str(r.get("semantic_note", ""))[:80],
                ])
            lines.append(tabulate(
                table_rows,
                headers=["Engine", "Status", "rows/s", "Notes"],
                tablefmt="github"
            ))
            lines.append("\n")
    else:
        lines.append("_No write results yet._\n")

    path.write_text("\n".join(lines))
    print(f"  Wrote: {path}")


def make_latency_charts(read_df: pd.DataFrame, charts_dir: Path):
    if read_df.empty:
        return
    for mode in MODES:
        sub = read_df[read_df["mode"] == mode]
        if sub.empty:
            continue
        fig, ax = plt.subplots(figsize=(14, 6))
        x       = np.arange(len(sub["query_id"].unique()))
        width   = 0.25
        queries = sorted(sub["query_id"].unique())

        for i, eng in enumerate(ENGINES):
            vals = [
                sub[(sub["query_id"] == q) & (sub["engine"] == eng)]["warm_median_s"].values
                for q in queries
            ]
            vals = [v[0] if len(v) > 0 else 0 for v in vals]
            ax.bar(x + i * width, vals, width, label=eng, color=COLORS.get(eng, "grey"), alpha=0.85)

        ax.set_xticks(x + width)
        ax.set_xticklabels([q.split("_")[0] for q in queries], rotation=45, ha="right")
        ax.set_ylabel("Warm Median Latency (s)")
        ax.set_title(f"Query Latency — {mode.upper()} mode (lower is better)")
        ax.legend()
        ax.set_yscale("log")
        plt.tight_layout()
        out = charts_dir / f"latency_{mode}.png"
        fig.savefig(out, dpi=150)
        plt.close(fig)
        print(f"  Chart: {out}")


def make_write_chart(write_df: pd.DataFrame, charts_dir: Path):
    if write_df.empty or "rows_per_s" not in write_df.columns:
        return
    sub = write_df[write_df["rows_per_s"].notna()].copy()
    sub["rows_per_s"] = pd.to_numeric(sub["rows_per_s"], errors="coerce")
    sub.dropna(subset=["rows_per_s"], inplace=True)
    if sub.empty:
        return

    fig, ax = plt.subplots(figsize=(10, 5))
    workloads = sorted(sub["workload"].unique())
    x = np.arange(len(workloads))
    width = 0.25

    for i, eng in enumerate(ENGINES):
        vals = [sub[(sub["workload"] == w) & (sub["engine"] == eng)]["rows_per_s"].values for w in workloads]
        vals = [v[0] if len(v) > 0 else 0 for v in vals]
        ax.bar(x + i * width, vals, width, label=eng, color=COLORS.get(eng, "grey"), alpha=0.85)

    ax.set_xticks(x + width)
    ax.set_xticklabels(workloads, rotation=15)
    ax.set_ylabel("rows / second (higher is better)")
    ax.set_title("Write Workload Throughput")
    ax.legend()
    plt.tight_layout()
    out = charts_dir / "write_throughput.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  Chart: {out}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", default="/opt1/olap_poc/poc/results")
    parser.add_argument("--out",     default="/opt1/olap_poc/poc/report")
    args = parser.parse_args()

    results_dir = Path(args.results)
    out_dir     = Path(args.out)
    charts_dir  = out_dir / "03_charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading results from {results_dir}...")
    df = load_results(results_dir)
    if df.empty:
        print("No results found. Run the benchmark first.")
        return

    read_df  = build_read_summary(df)
    write_df = build_write_summary(df)

    write_csv(df,                  out_dir / "01_raw_results.csv")
    write_summary_md(read_df, write_df, out_dir / "02_summary_table.md")
    make_latency_charts(read_df, charts_dir)
    make_write_chart(write_df, charts_dir)

    print("\nAnalysis complete. Open report/02_summary_table.md for results.")


if __name__ == "__main__":
    main()
