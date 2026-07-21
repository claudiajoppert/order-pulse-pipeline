"""Throughput/latency benchmark harness: real measured numbers, not invented
ones. Run this directly (`python benchmark.py`) to reproduce the numbers
quoted in the README.
"""

import os
import time

import numpy as np
import pandas as pd

from event_generator import generate_batch
from pipeline import process_batch_multiprocessing, process_batch_sequential


def _percentiles(latencies: list[float]) -> tuple[float, float]:
    arr = np.array(latencies) * 1000  # seconds -> ms
    return float(np.percentile(arr, 50)), float(np.percentile(arr, 99))


def run_benchmark(event_count: int = 20_000, worker_counts: list[int] | None = None) -> pd.DataFrame:
    """Run the same batch of events sequentially and across several worker
    counts, measuring wall-clock throughput and per-event latency percentiles.
    """
    if worker_counts is None:
        max_workers = os.cpu_count() or 4
        worker_counts = sorted(set([1, 2, 4, max_workers]))

    events = generate_batch(event_count, seed=42)
    rows = []

    start = time.perf_counter()
    _, seq_latencies = process_batch_sequential(events)
    seq_elapsed = time.perf_counter() - start
    p50, p99 = _percentiles(seq_latencies)
    rows.append({
        "mode": "sequential",
        "workers": 1,
        "events": event_count,
        "elapsed_sec": seq_elapsed,
        "events_per_sec": event_count / seq_elapsed,
        "p50_latency_ms": p50,
        "p99_latency_ms": p99,
    })

    for workers in worker_counts:
        if workers == 1:
            continue
        start = time.perf_counter()
        _, mp_latencies = process_batch_multiprocessing(events, num_workers=workers)
        mp_elapsed = time.perf_counter() - start
        p50, p99 = _percentiles(mp_latencies)
        rows.append({
            "mode": "multiprocessing",
            "workers": workers,
            "events": event_count,
            "elapsed_sec": mp_elapsed,
            "events_per_sec": event_count / mp_elapsed,
            "p50_latency_ms": p50,
            "p99_latency_ms": p99,
        })

    return pd.DataFrame(rows)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=20_000)
    parser.add_argument("--json", action="store_true", help="print results as JSON instead of a table")
    args = parser.parse_args()

    results = run_benchmark(event_count=args.events)

    if args.json:
        print(results.to_json(orient="records"))
    else:
        pd.set_option("display.float_format", lambda x: f"{x:,.2f}")
        print(results.to_string(index=False))
