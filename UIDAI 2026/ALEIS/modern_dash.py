import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path

# =====================================================
# PAGE CONFIG
# =====================================================
st.set_page_config(
    page_title="ALEIS Intelligence Center",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# =====================================================
# SAAS CSS (SPACE OPTIMISED)
# =====================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

* { font-family: 'Inter', sans-serif; }
#MainMenu, footer, header { visibility: hidden; }

.stApp { background-color: #f8fafc; }

.main-header {
    background: white;
    border: 1px solid #e5e7eb;
    padding: 1.4rem 2rem;
    border-radius: 14px;
    margin-bottom: 1rem;
}

.header-title {
    font-size: 2rem;
    font-weight: 800;
    color: #111827;
    margin: 0;
}

.header-subtitle {
    font-size: 0.9rem;
    color: #6b7280;
}

.kpi-card {
    background: white;
    border: 1px solid #e5e7eb;
    padding: 1.1rem 1.3rem;
    border-radius: 12px;
    box-shadow: 0 1px 6px rgba(0,0,0,0.04);
}

.kpi-label {
    font-size: 0.7rem;
    font-weight: 600;
    color: #6b7280;
    text-transform: uppercase;
}

.kpi-value {
    font-size: 1.6rem;
    font-weight: 800;
    color: #111827;
    margin-top: 0.3rem;
}

.chart-container {
    background: white;
    border: 1px solid #e5e7eb;
    padding: 1rem 1.2rem;
    border-radius: 14px;
    box-shadow: 0 1px 8px rgba(0,0,0,0.04);
}
</style>
""", unsafe_allow_html=True)

# =====================================================
# DATA
# =====================================================
@st.cache_data
def load_data():
    path = Path("data/processed/monthly/demo_indicators.csv")
    if not path.exists():
        st.error("Data file not found")
        st.stop()
    return pd.read_csv(path)

df = load_data()

# =====================================================
# HEADER
# =====================================================
st.markdown("""
<div class="main-header">
    <h1 class="header-title">ALEIS Intelligence Center</h1>
    <p class="header-subtitle">
        Aadhaar Life-Event Intelligence System · National Monitoring Dashboard
    </p>
</div>
""", unsafe_allow_html=True)

# =====================================================
# KPI CALCULATIONS
# =====================================================
total_enrolments = int(df["enrolments"].sum())
total_updates = int(df["total_updates"].sum())
avg_lepi = df["lepi"].mean()
alerts = int(df["anomaly_flag"].sum())

high_alerts = len(df[df["lepi"] > 80])
medium_alerts = len(df[(df["lepi"] > 60) & (df["lepi"] <= 80)])

peak_lepi = df.groupby(["year","month"])["lepi"].mean().max()
top_district = (
    df.groupby("district")["total_updates"]
    .sum()
    .idxmax()
)

# =====================================================
# KPI ROW 1
# =====================================================
r1 = st.columns(4)
metrics_1 = [
    ("Total Enrolments", f"{total_enrolments:,}"),
    ("Total Updates", f"{total_updates:,}"),
    ("Average LEPI", f"{avg_lepi:.2f}"),
    ("Active Alerts", alerts),
]

for col, (label, value) in zip(r1, metrics_1):
    with col:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
        </div>
        """, unsafe_allow_html=True)

# =====================================================
# KPI ROW 2
# =====================================================
r2 = st.columns(4)
metrics_2 = [
    ("High Severity Alerts", high_alerts),
    ("Medium Severity Alerts", medium_alerts),
    ("Peak LEPI Score", f"{peak_lepi:.1f}"),
    ("Top District", top_district),
]

for col, (label, value) in zip(r2, metrics_2):
    with col:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# =====================================================
# MAIN CHARTS (SPACE OPTIMISED)
# =====================================================
left, right = st.columns([2.2, 1])

# ---------- LEPI TREND ----------
with left:
    st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
    st.markdown("### LEPI Temporal Trend")

    df_t = (
        df.groupby(["year","month"])
        .agg(lepi=("lepi","mean"))
        .reset_index()
        .sort_values(["year","month"])
    )
    df_t["period"] = df_t["year"].astype(str) + "-" + df_t["month"].astype(str).str.zfill(2)
    df_t["trend"] = df_t["lepi"].rolling(3, min_periods=1).mean()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_t["period"],
        y=df_t["lepi"],
        name="LEPI",
        line=dict(color="#4f46e5", width=2.8)
    ))
    fig.add_trace(go.Scatter(
        x=df_t["period"],
        y=df_t["trend"],
        name="Trend",
        line=dict(color="#9ca3af", dash="dash")
    ))

    fig.update_layout(
        height=300,
        margin=dict(l=40,r=20,t=10,b=40),
        hovermode="x unified",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(title="Period"),
        yaxis=dict(title="LEPI Score"),
        legend=dict(orientation="h", y=1.05)
    )

    st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

# ---------- DONUT ----------
with right:
    st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
    st.markdown("### Top Districts by Updates")

    dist = (
        df.groupby("district")["total_updates"]
        .sum()
        .sort_values(ascending=False)
        .head(8)
        .reset_index()
    )

    pie = go.Figure(go.Pie(
        labels=dist["district"],
        values=dist["total_updates"],
        hole=0.6
    ))

    pie.update_layout(
        height=300,
        margin=dict(l=10,r=10,t=10,b=10)
    )

    st.plotly_chart(pie, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

# =====================================================
# ANOMALY TABLE
# =====================================================
st.markdown("<br>", unsafe_allow_html=True)
st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
st.markdown("### 🚨 District-Level Anomaly Intelligence")

anom = df[df["anomaly_flag"] == True].copy()
if not anom.empty:
    anom["Period"] = anom["year"].astype(str) + "-" + anom["month"].astype(str).str.zfill(2)
    st.dataframe(
        anom[["state","district","Period","lepi","total_updates","enrolments"]],
        use_container_width=True,
        height=320
    )
else:
    st.info("No anomalies detected")

st.markdown("</div>", unsafe_allow_html=True)

# =====================================================
# FOOTER
# =====================================================
st.markdown("""
<p style="text-align:center; color:#9ca3af; font-size:0.8rem;">
ALEIS Intelligence Center · Production SaaS Dashboard · UIDAI 2026
</p>
""", unsafe_allow_html=True)
