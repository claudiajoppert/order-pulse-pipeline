"""Order Pulse — a high-throughput order-event pipeline: synthetic event
generation, parallel feature processing, DuckDB windowed aggregation, and a
real, measured throughput/latency benchmark across worker counts.
"""

import json
import subprocess
import sys

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from event_generator import generate_batch
from pipeline import aggregate_metrics, load_into_duckdb, process_batch_sequential

st.set_page_config(page_title="Order Pulse", page_icon="⚡", layout="wide")

st.title("⚡ Order Pulse")
st.markdown(
    "A synthetic grocery order-event pipeline: generate events, run them through a "
    "feature-processing stage, load into DuckDB, and aggregate rolling operational "
    "metrics — with a real, measured throughput benchmark across worker counts."
)

tab_live, tab_benchmark = st.tabs(["Live simulation", "Throughput benchmark"])

with tab_live:
    st.markdown("### Simulate an event batch")
    col1, col2 = st.columns(2)
    with col1:
        event_count = st.slider("Number of events", 500, 20_000, 5_000, step=500)
    with col2:
        seed = st.number_input("Random seed", value=42, step=1)

    if st.button("Run simulation", type="primary"):
        with st.spinner(f"Generating and processing {event_count:,} events..."):
            events = generate_batch(event_count, seed=seed)
            processed, latencies = process_batch_sequential(events)

            con = duckdb.connect(":memory:")
            load_into_duckdb(con, processed)
            agg = aggregate_metrics(con)

        st.session_state["agg"] = agg
        st.session_state["latencies"] = latencies
        st.session_state["event_count"] = event_count

    if "agg" in st.session_state:
        agg = st.session_state["agg"]
        latencies_ms = pd.Series(st.session_state["latencies"]) * 1000

        c1, c2, c3 = st.columns(3)
        c1.metric("Events processed", f"{st.session_state['event_count']:,}")
        c2.metric("Overall late rate", f"{(agg['event_count'] * agg['late_rate']).sum() / agg['event_count'].sum():.1%}")
        c3.metric("p99 processing latency", f"{latencies_ms.quantile(0.99):.2f} ms")

        st.markdown("#### Late-delivery rate by store & category")
        top = agg.sort_values("late_rate", ascending=False).head(15)
        fig = go.Figure(go.Bar(
            x=top["late_rate"],
            y=[f"{r.store_id} / {r.category}" for r in top.itertuples()],
            orientation="h",
            marker_color="#d03b3b",
        ))
        fig.update_layout(
            height=450,
            xaxis_title="Late rate",
            margin=dict(l=10, r=10, t=20, b=10),
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### Full aggregation")
        st.dataframe(agg, use_container_width=True, hide_index=True)
    else:
        st.info("Click **Run simulation** to generate events and see rolling metrics.")

with tab_benchmark:
    st.markdown("### Measured throughput across worker counts")
    st.caption(
        "Runs `benchmark.py` as a subprocess (multiprocessing.Pool doesn't play well "
        "inside Streamlit's script-execution model on macOS/Windows) and reports the "
        "real numbers it measures — nothing here is hardcoded."
    )
    bench_events = st.slider("Events per benchmark run", 2_000, 30_000, 15_000, step=1_000)

    if st.button("Run benchmark"):
        with st.spinner(f"Benchmarking {bench_events:,} events across worker counts (this actually runs the pipeline, ~10-20s)..."):
            result = subprocess.run(
                [sys.executable, "benchmark.py", "--events", str(bench_events), "--json"],
                capture_output=True,
                text=True,
                timeout=120,
            )
        if result.returncode != 0:
            st.error(f"Benchmark failed:\n{result.stderr}")
        else:
            bench_df = pd.DataFrame(json.loads(result.stdout))
            st.session_state["bench_df"] = bench_df

    if "bench_df" in st.session_state:
        bench_df = st.session_state["bench_df"]
        fig = go.Figure(go.Bar(
            x=bench_df["workers"].astype(str) + " worker(s)\n(" + bench_df["mode"] + ")",
            y=bench_df["events_per_sec"],
            marker_color="#2a78d6",
        ))
        fig.update_layout(
            height=350,
            yaxis_title="Events / second",
            margin=dict(l=10, r=10, t=20, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(
            bench_df[["mode", "workers", "events", "events_per_sec", "p50_latency_ms", "p99_latency_ms"]],
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("Click **Run benchmark** to measure real throughput on this machine.")

st.markdown("---")
st.caption(
    "All data is synthetic. Parallelism is applied to the CPU-bound feature-processing "
    "stage only; DuckDB is loaded with a single bulk insert per run since DuckDB doesn't "
    "safely support concurrent multi-process writers to one file."
)
