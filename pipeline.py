"""Event processing + DuckDB aggregation.

Design note: DuckDB supports one writer process at a time — concurrent
multi-process writes to the same database file are not safely supported.
So the parallelism here is applied to the CPU-bound per-event *processing*
stage (feature computation or the events, run across a multiprocessing
pool), while the DuckDB load is a single fast bulk insert done once by the
caller after processing completes. This is both correct and the realistic
shape of a real pipeline: parallel transform, single-writer load.
"""

import math
import multiprocessing
import time

import duckdb
import pandas as pd

EVENTS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id VARCHAR,
    order_id VARCHAR,
    store_id VARCHAR,
    category VARCHAR,
    event_type VARCHAR,
    items_count INTEGER,
    is_late BOOLEAN,
    is_substitution BOOLEAN,
    risk_score DOUBLE,
    generated_at DOUBLE
)
"""


FEATURE_WORK_MULTIPLIER = 120  # tuned so per-event cost outweighs multiprocessing IPC overhead


def _compute_risk_score(items_count: int, is_late: bool, is_substitution: bool) -> float:
    """A deliberately non-trivial per-event feature computation, standing in
    for real-world processing cost (parsing a larger payload, multiple
    derived features per line item, etc.) so parallelism has something
    real to speed up rather than being dominated by IPC overhead.
    """
    iterations = items_count * FEATURE_WORK_MULTIPLIER
    score = 0.0
    for i in range(1, iterations + 1):
        score += math.sin(i) ** 2 + math.log(i + 1)
    score = score / max(iterations, 1)
    if is_late:
        score += 15.0
    if is_substitution:
        score += 5.0
    return round(score, 4)


def process_event(event: dict) -> tuple[dict, float]:
    """Process one event, returning (processed_event, elapsed_seconds).

    The per-event elapsed time is what benchmark.py uses to compute
    latency percentiles.
    """
    start = time.perf_counter()
    risk_score = _compute_risk_score(event["items_count"], event["is_late"], event["is_substitution"])
    processed = {**event, "risk_score": risk_score}
    elapsed = time.perf_counter() - start
    return processed, elapsed


def process_batch_sequential(events: list[dict]) -> tuple[list[dict], list[float]]:
    results, latencies = [], []
    for event in events:
        processed, elapsed = process_event(event)
        results.append(processed)
        latencies.append(elapsed)
    return results, latencies


def process_batch_multiprocessing(events: list[dict], num_workers: int) -> tuple[list[dict], list[float]]:
    with multiprocessing.Pool(processes=num_workers) as pool:
        pairs = pool.map(process_event, events, chunksize=max(1, len(events) // (num_workers * 4)))
    results = [p[0] for p in pairs]
    latencies = [p[1] for p in pairs]
    return results, latencies


def load_into_duckdb(con: duckdb.DuckDBPyConnection, processed_events: list[dict]) -> None:
    """Single bulk insert — the one writer touching the DuckDB file.

    Uses `INSERT ... BY NAME` deliberately: a plain positional `SELECT *`
    silently mismatches columns whenever the DataFrame's column order
    (dict insertion order) doesn't exactly match the table schema — which
    it won't here, since `risk_score` is appended after `generated_at` in
    process_event's output dict but declared before it in the schema.
    """
    con.execute(EVENTS_TABLE_SCHEMA)
    df = pd.DataFrame(processed_events)
    con.execute("INSERT INTO events BY NAME SELECT * FROM df")


def aggregate_metrics(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Windowed rollups: order rate, late-delivery rate, substitution rate per store/category."""
    return con.execute("""
        SELECT
            store_id,
            category,
            COUNT(*) AS event_count,
            AVG(CAST(is_late AS INTEGER)) AS late_rate,
            AVG(CAST(is_substitution AS INTEGER)) AS substitution_rate,
            AVG(risk_score) AS avg_risk_score
        FROM events
        GROUP BY store_id, category
        ORDER BY late_rate DESC
    """).df()
