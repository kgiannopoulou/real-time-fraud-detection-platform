"""Real-time fraud monitor (Streamlit).

    streamlit run dashboard/app.py          # http://localhost:8501

Reads the PostgreSQL serving store and refreshes itself every few seconds.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

if __package__ in (None, ""):  # `streamlit run dashboard/app.py`
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import queries

# Palette (dark surface): one hue for volume, the reserved "critical" status
# color for fraud. Measures with different scales get separate charts.
SURFACE = "#1a1a19"
GRID = "#2e2e2b"
TEXT = "#c3c2b7"
MUTED = "#8a897f"
VOLUME = "#3987e5"
FRAUD = "#d03b3b"
RANGES = {"15 min": 15, "1 hour": 60, "6 hours": 360, "24 hours": 1440}

st.set_page_config(page_title="Fraud Monitor", page_icon="🛡️", layout="wide")
st.markdown(
    """<style>
    [data-testid="stMetricValue"] {font-size: 2rem;}
    [data-testid="stMetric"] {background: #1a1a19; border: 1px solid #2e2e2b; border-radius: 8px;
                              padding: 12px 16px;}
    .block-container {padding-top: 1.6rem;}
    </style>""",
    unsafe_allow_html=True,
)


def style(fig: go.Figure, title: str, height: int = 260) -> go.Figure:
    fig.update_layout(
        title={"text": title, "font": {"size": 14, "color": TEXT}, "x": 0, "xanchor": "left"},
        height=height,
        margin={"l": 8, "r": 8, "t": 40, "b": 8},
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        font={"color": MUTED, "size": 12},
        showlegend=False,
        hoverlabel={"bgcolor": "#262624", "font_color": "#f0efec", "bordercolor": GRID},
        bargap=0.25,
    )
    fig.update_xaxes(showgrid=False, linecolor=GRID, zeroline=False)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, rangemode="tozero")
    return fig


def bar(
    x,
    y,
    color: str,
    title: str,
    orientation: str = "v",
    hover: str = "%{y:,}<extra></extra>",
    height: int = 260,
) -> go.Figure:
    fig = go.Figure(
        go.Bar(
            x=x, y=y, orientation=orientation, marker={"color": color, "cornerradius": 4}, hovertemplate=hover
        )
    )
    return style(fig, title, height)


def header() -> int:
    left, right = st.columns([4, 1], vertical_alignment="center")
    left.markdown("## 🛡️ Real-Time Fraud Monitor")
    label = right.selectbox("Time range", list(RANGES), index=1, label_visibility="collapsed")
    return RANGES[label]


def kpi_row(minutes: int) -> None:
    k = queries.kpis(minutes)
    tx, frauds = int(k["transactions"] or 0), int(k["frauds"] or 0)
    cols = st.columns(6)
    cols[0].metric("Transactions", f"{tx:,}")
    cols[1].metric("Flagged as fraud", f"{frauds:,}")
    cols[2].metric("Blocked amount", f"€{float(k['blocked_eur'] or 0):,.0f}")
    cols[3].metric("Fraud rate", f"{(frauds / tx if tx else 0):.2%}")
    p95 = k["p95_latency_ms"]
    cols[4].metric("p95 latency", f"{p95 / 1000:.1f} s" if p95 == p95 and p95 is not None else "–")
    cols[5].metric("Model", f"v{k['model_version']}" if k["model_version"] else "–")


def trend_row(minutes: int) -> None:
    df = queries.per_minute(minutes)
    left, right = st.columns(2)
    if df.empty:
        left.info("Waiting for scored transactions…")
        return
    # Idle minutes are real zeros, not gaps to interpolate across.
    end = pd.Timestamp.now(tz="UTC").floor("min")
    index = pd.date_range(end=end, periods=minutes, freq="min")
    df["minute"] = pd.to_datetime(df["minute"], utc=True)
    df = df.set_index("minute").reindex(index.union(df["minute"]), fill_value=0)
    df = df[df.index >= index[0]].rename_axis("minute").reset_index()
    fig = go.Figure(
        go.Scatter(
            x=df["minute"],
            y=df["transactions"],
            mode="lines",
            line={"color": VOLUME, "width": 2},
            fill="tozeroy",
            fillcolor="rgba(57,135,229,0.15)",
            hovertemplate="%{x|%H:%M}<br>%{y:,} transactions<extra></extra>",
        )
    )
    fig.update_layout(hovermode="x unified")
    left.plotly_chart(style(fig, "Transactions per minute"), width="stretch")
    right.plotly_chart(
        bar(
            df["minute"],
            df["frauds"],
            FRAUD,
            "Fraud per minute",
            hover="%{x|%H:%M}<br>%{y} flagged<extra></extra>",
        ),
        width="stretch",
    )


def distribution_row(minutes: int) -> None:
    c1, c2, c3 = st.columns(3)
    probs = queries.probabilities(minutes)
    if not probs.empty:
        fig = go.Figure(
            go.Histogram(
                x=probs["fraud_probability"],
                xbins={"start": 0, "end": 1, "size": 0.05},
                marker={"color": VOLUME, "cornerradius": 3},
                hovertemplate="p %{x}<br>%{y:,} transactions<extra></extra>",
            )
        )
        fig.update_yaxes(type="log")
        c1.plotly_chart(style(fig, "Fraud probability distribution (log count)"), width="stretch")
    for col, column, title in (
        (c2, "country", "Fraud by country"),
        (c3, "merchant_category", "Fraud by merchant category"),
    ):
        df = queries.fraud_by(column, minutes)
        if df.empty:
            col.caption(f"{title}: no fraud in range")
            continue
        df = df.iloc[::-1]
        col.plotly_chart(
            bar(
                df["frauds"],
                df["label"],
                FRAUD,
                title,
                orientation="h",
                hover="%{y}: %{x} flagged<extra></extra>",
            ),
            width="stretch",
        )


def hour_row(minutes: int) -> None:
    df = queries.fraud_by_hour(minutes)
    if df.empty:
        return
    full = pd.DataFrame({"hour": range(24)}).merge(df, how="left").fillna(0)
    st.plotly_chart(
        bar(
            full["hour"],
            full["frauds"],
            FRAUD,
            "Fraud by hour of day (UTC)",
            hover="%{x}:00 — %{y} flagged<extra></extra>",
            height=220,
        ),
        width="stretch",
    )


def live_table() -> None:
    st.markdown("#### Live transactions")
    only_fraud = st.toggle("Only fraud", value=False)
    df = queries.latest(30, only_fraud=only_fraud)
    if df.empty:
        st.info("No transactions yet.")
        return
    view = pd.DataFrame(
        {
            "Time": pd.to_datetime(df["event_time"]).dt.strftime("%H:%M:%S"),
            "User": df["user_id"],
            "Amount": df["amount_eur"],
            "Category": df["merchant_category"],
            "Country": df["country"],
            "Risk": df["fraud_probability"],
            "Status": df["prediction"].map({"FRAUD": "🚨 FRAUD", "LEGIT": "✓ normal"}),
            "Transaction": df["transaction_id"],
        }
    )
    st.dataframe(
        view,
        hide_index=True,
        width="stretch",
        height=420,
        column_config={
            "Amount": st.column_config.NumberColumn(format="€%.2f"),
            "Risk": st.column_config.ProgressColumn(format="percent", min_value=0, max_value=1),
        },
    )


def detail_panel() -> None:
    st.markdown("#### Transaction detail")
    recent = queries.latest(50, only_fraud=True)["transaction_id"].tolist()
    options = recent
    if not options:
        st.caption("No flagged transactions yet.")
        return
    tx_id = st.selectbox("Flagged transaction", options, label_visibility="collapsed")
    d = queries.transaction_detail(tx_id)
    if d is None:
        return
    prob = float(d["fraud_probability"])
    st.markdown(
        f"**{d['transaction_id']}** · {d['user_id']} · €{float(d['amount_eur']):,.2f} "
        f"{d['merchant_category']} in {d['country']} · model v{d['model_version']}"
    )
    color = FRAUD if prob >= 0.5 else VOLUME
    st.markdown(
        f"""<div style="margin:4px 0 2px">Fraud probability <b>{prob:.0%}</b></div>
        <div style="background:#2e2e2b;border-radius:4px;height:10px">
          <div style="width:{prob:.1%};background:{color};height:10px;border-radius:4px"></div></div>""",
        unsafe_allow_html=True,
    )
    factors = (
        d["risk_factors"] if isinstance(d["risk_factors"], list) else json.loads(d["risk_factors"] or "[]")
    )
    st.markdown("**Reasons**")
    for reason in factors or ["no single dominant factor"]:
        st.markdown(f"↑ {reason}")
    feats = d["features"] if isinstance(d["features"], dict) else json.loads(d["features"] or "{}")
    with st.expander("Model features"):
        st.dataframe(
            pd.DataFrame({"feature": list(feats), "value": list(feats.values())}),
            hide_index=True,
            width="stretch",
        )


def alerts_tab() -> None:
    status = st.radio("Status", ["open", "confirmed", "dismissed"], horizontal=True)
    df = queries.alerts(status)
    if df.empty:
        st.info(f"No {status} alerts.")
        return
    for row in df.head(20).itertuples():
        icon = "🔴" if row.severity == "critical" else "🟠"
        factors = row.risk_factors if isinstance(row.risk_factors, list) else json.loads(row.risk_factors)
        with st.container(border=True):
            a, b = st.columns([5, 2])
            a.markdown(
                f"{icon} **{row.severity.upper()}** · `{row.transaction_id}` · {row.user_id} · "
                f"€{row.amount_eur:,.2f} · p={row.fraud_probability:.2f}"
            )
            a.caption(" · ".join(factors))
            if status == "open":
                if b.button("Confirm fraud", key=f"c{row.alert_id}"):
                    queries.set_alert_status(row.alert_id, "confirmed")
                    st.rerun()
                if b.button("Dismiss", key=f"d{row.alert_id}"):
                    queries.set_alert_status(row.alert_id, "dismissed")
                    st.rerun()


minutes = header()
monitor, alerts_view = st.tabs(["Live monitor", "Alerts"])

with monitor:

    @st.fragment(run_every="5s")
    def live() -> None:
        kpi_row(minutes)
        trend_row(minutes)
        distribution_row(minutes)
        hour_row(minutes)

    live()
    left, right = st.columns([3, 2])
    with left:
        st.fragment(run_every="5s")(live_table)()
    with right:
        detail_panel()

with alerts_view:
    alerts_tab()
