#!/usr/bin/env python3
"""
NIL Intelligence Dashboard — Collegiate Mobile
------------------------------------------------

Focus:
 • Where NIL activity is concentrated
 • Which schools, athletes, states, and divisions matter
 • Market share distributions (NO PIE CHARTS)
"""

import pandas as pd
import streamlit as st
import altair as alt

# ============================================================
# LOAD & FILTER DATA
# ============================================================
df = pd.read_csv("data/processed/on3_nil_deals_all.csv")
df_dedupe = pd.read_csv("data/processed/on3_nil_athlete_values.csv")

df["deal_date"] = pd.to_datetime(df["deal_date"], errors="coerce")
df = df[(df["deal_date"].dt.year >= 2022) & (df["deal_date"].dt.year <= 2025)]

df["sport_name"] = df["sport_name"].fillna("Unknown")
df["player_state"] = df["player_state"].fillna("Unknown")

# ============================================================
# PAGE SETUP
# ============================================================
st.set_page_config(page_title="NIL Intelligence Dashboard", layout="wide")
st.title("📊 NIL Intelligence Dashboard — Collegiate Mobile (2022–2025)")

st.write("""
This dashboard reflects **mature NIL activity from 2022–2025**, filter to exclude partial 2025 data.  
It highlights where NIL deal **volume, value, and influence** concentrate — useful for
prioritizing marketing and partnership strategy.
""")

# ============================================================
# FILTERS
# ============================================================
st.header("Filters")

c1, c2, c3 = st.columns(3)

with c1:
    selected_school = st.multiselect(
        "Team Committed",
        options=sorted(df["team_committed"].dropna().unique()),
        default=[]
    )

with c2:
    selected_sports = st.multiselect(
        "Sport",
        options=sorted(df["sport_name"].dropna().unique()),
        default=[]
    )

with c3:
    year_range = st.slider(
        "Year Range",
        min_value=2022,
        max_value=2025,
        value=(2022, 2025)
    )

# ------------------------------------------------------------
# APPLY FILTERS
# ------------------------------------------------------------
filtered_df = df.copy()
filtered_dedupe = df_dedupe.copy()

# School filter
if selected_school:
    filtered_df = filtered_df[filtered_df["team_committed"].isin(selected_school)]
    filtered_dedupe = filtered_dedupe[filtered_dedupe["team_committed"].isin(selected_school)]

# Sport filter
if selected_sports:
    filtered_df = filtered_df[filtered_df["sport_name"].isin(selected_sports)]
    filtered_dedupe = filtered_dedupe[filtered_dedupe["sport_name"].isin(selected_sports)]

# Year filter (RAW DEALS ONLY)
filtered_df = filtered_df[
    (filtered_df["deal_date"].dt.year >= year_range[0]) &
    (filtered_df["deal_date"].dt.year <= year_range[1])
]


# ============================================================
# KPI SNAPSHOT
# ============================================================
st.header("📌 NIL Market Snapshot")

k1, k2, k3, k4 = st.columns(4)

k1.metric("Total NIL Deals", f"{len(filtered_df):,}")
k2.metric("Schools", filtered_df["team_committed"].nunique())
k3.metric("Athletes", filtered_df["player_key"].nunique())
k4.metric(
    "Reported NIL Value",
    f"${filtered_dedupe['deal_value'].dropna().sum():,.0f}"
)

# ============================================================
# TOP SCHOOLS + TIME SERIES
# ============================================================
st.header("🏫 NIL Market Overview")

col1, col2 = st.columns(2)

# -------------------------
# TOP SCHOOLS
# -------------------------
school_summary = (
    filtered_df.groupby("team_committed")
    .agg(
        deals=("deal_key", "count"),
        athletes=("player_key", "nunique")
    )
    .sort_values("deals", ascending=False)
    .head(10)
    .reset_index()
)

school_bars = (
    alt.Chart(school_summary)
    .mark_bar(color="#2563eb")
    .encode(
        x="deals:Q",
        y=alt.Y("team_committed:N", sort="-x"),
        tooltip=["team_committed", "deals", "athletes"]
    )
)

school_labels = (
    alt.Chart(school_summary)
    .mark_text(align="left", dx=4, color="white", fontWeight="bold")
    .encode(
        x="deals:Q",
        y=alt.Y("team_committed:N", sort="-x"),
        text=alt.Text("deals:Q", format=",")
    )
)

with col1:
    st.subheader("Top NIL Schools")
    st.altair_chart((school_bars + school_labels).properties(height=350),
                     use_container_width=True)

# -------------------------
# TIME SERIES
# -------------------------
filtered_df["deal_month"] = filtered_df["deal_date"].dt.to_period("M").dt.to_timestamp()

time_series = (
    filtered_df.groupby("deal_month")
    .size()
    .reset_index(name="deals")
)

time_line = (
    alt.Chart(time_series)
    .mark_line(point=True, strokeWidth=3, color="#ef4444")
    .encode(
        x="deal_month:T",
        y="deals:Q",
        tooltip=["deal_month", "deals"]
    )
)

time_labels = (
    alt.Chart(time_series)
    .mark_text(dy=-8, fontSize=10, color="#374151")
    .encode(
        x="deal_month:T",
        y="deals:Q",
        text=alt.Text("deals:Q", format=",")
    )
)

with col2:
    st.subheader("NIL Deals Over Time")
    st.altair_chart((time_line + time_labels).properties(height=350),
                     use_container_width=True)

# ============================================================
# NIL DEAL SHARE BY DIVISION
# ============================================================
st.header("🏟 NIL Deal Share by Division")

division_df = (
    filtered_df["sport_name"]
    .value_counts()
    .reset_index()
)

division_df.columns = ["division", "deal_count"]
total_deals = division_df["deal_count"].sum()
division_df["share"] = division_df["deal_count"] / total_deals

left, right = st.columns(2)

# ------------------------------------------------------------
# LEFT: Top Brands by Deal Volume (All Deals)
# ------------------------------------------------------------
brand_volume = (
    filtered_df
    .groupby("company_name")
    .size()
    .reset_index(name="deal_count")
    .sort_values("deal_count", ascending=False)
    .head(10)
)

brand_volume_bars = (
    alt.Chart(brand_volume)
    .mark_bar(color="#6b7280")
    .encode(
        x=alt.X("deal_count:Q", title="Number of NIL Deals"),
        y=alt.Y("company_name:N", sort="-x", title="Brand"),
        tooltip=[
            "company_name",
            alt.Tooltip("deal_count:Q", title="Deals", format=",")
        ]
    )
)

brand_volume_labels = (
    alt.Chart(brand_volume)
    .mark_text(
        align="left",
        dx=4,
        color="white",
        fontWeight="bold"
    )
    .encode(
        x="deal_count:Q",
        y=alt.Y("company_name:N", sort="-x"),
        text=alt.Text("deal_count:Q", format=",")
    )
)

with left:
    st.subheader("Most Active Brands (Deal Volume)")
    st.altair_chart(
        (brand_volume_bars + brand_volume_labels).properties(height=350),
        use_container_width=True
    )
    st.caption(
        "Highlights brands running many smaller activations — "
        "often local, regional, or performance-based sponsorships."
    )


with right:
    st.subheader("Share of Total NIL Deals")

    bars = (
        alt.Chart(division_df)
        .mark_bar(color="#3b82f6")
        .encode(
            x=alt.X("division:N",
                    sort=alt.SortField(field="share", order="descending")),
            y=alt.Y("share:Q",
                    axis=alt.Axis(format="%")),
            tooltip=[
                "division",
                alt.Tooltip("deal_count:Q", format=","),
                alt.Tooltip("share:Q", format=".1%")
            ]
        )
    )

    labels = (
        alt.Chart(division_df)
        .mark_text(dy=-6, color="white", fontWeight="bold")
        .encode(
            x=alt.X("division:N",
                    sort=alt.SortField(field="share", order="descending")),
            y="share:Q",
            text=alt.Text("share:Q", format=".1%")
        )
    )

    st.altair_chart((bars + labels).properties(height=420),
                     use_container_width=True)

# ============================================================
# TOP NIL ATHLETES — VOLUME VS VALUE
# ============================================================
st.header("🏅 Top NIL Athletes — Volume vs Value")

col1, col2 = st.columns(2)

# -------------------------
# DEAL COUNT
# -------------------------
athlete_volume = (
    filtered_df.groupby(["player_key", "player_name"])
    .size()
    .reset_index(name="deal_count")
    .sort_values("deal_count", ascending=False)
    .head(10)
)

volume_bars = (
    alt.Chart(athlete_volume)
    .mark_bar(color="#7c3aed")
    .encode(
        x="deal_count:Q",
        y=alt.Y("player_name:N", sort="-x"),
        tooltip=["player_name", "deal_count"]
    )
)

volume_labels = (
    alt.Chart(athlete_volume)
    .mark_text(align="left", dx=4, color="white", fontWeight="bold")
    .encode(
        x="deal_count:Q",
        y=alt.Y("player_name:N", sort="-x"),
        text=alt.Text("deal_count:Q", format=",")
    )
)

with col1:
    st.subheader("Most NIL Deals (Volume)")
    st.altair_chart((volume_bars + volume_labels).properties(height=350),
                     use_container_width=True)

# -------------------------
# DEAL VALUE (REPORTED)
# -------------------------
df_money = filtered_df[filtered_df["deal_amount"].notnull()]

athlete_value = (
    df_money.groupby(["player_key", "player_name"])
    .agg(
        total_value=("deal_amount", "mean"), # Fix aggregation method
        deal_count=("deal_amount", "count"),
        avg_value=("deal_amount", "mean")
    )
    .reset_index()
    .sort_values("total_value", ascending=False)
    .head(10)
)

value_bars = (
    alt.Chart(athlete_value)
    .mark_bar(color="#2563eb")
    .encode(
        x=alt.X("total_value:Q",
                axis=alt.Axis(format="~s")),
        y=alt.Y("player_name:N", sort="-x"),
        tooltip=[
            "player_name",
            alt.Tooltip("total_value:Q", format="$,.0f"),
            "deal_count",
            alt.Tooltip("avg_value:Q", format="$,.0f")
        ]
    )
)

value_labels = (
    alt.Chart(athlete_value)
    .mark_text(align="left", dx=4, color="white", fontWeight="bold")
    .encode(
        x="total_value:Q",
        y=alt.Y("player_name:N", sort="-x"),
        text=alt.Text("total_value:Q", format="$,.0f")
    )
)

with col2:
    st.subheader("Highest NIL Value (Reported $)")
    st.altair_chart((value_bars + value_labels).properties(height=350),
                     use_container_width=True)

# ============================================================
# SCHOOL-LEVEL NIL SUMMARY TABLE (DEDUPED)
# ============================================================
st.header("🏫 School-Level NIL Summary (Deduped NIL Values)")

# Only deals with reported NIL values
school_money = (
    filtered_dedupe
    .loc[filtered_dedupe["deal_value"].notnull()]
    .groupby(
        ["team_committed", "player_key"],
        as_index=False
    )
    .agg(
        deal_value=("deal_value", "max")  # defensive dedupe
    )
)

if school_money.empty:
    st.warning("No reported NIL values available.")
else:
    # Aggregate to school level
    school_table = (
        school_money
        .groupby("team_committed")
        .agg(
            total_value=("deal_value", "sum"),
            avg_value=("deal_value", "mean"),
            median_value=("deal_value", "median"),
            deal_count=("deal_value", "count"),
            athletes=("player_key", "nunique")
        )
        .reset_index()
        .sort_values("total_value", ascending=False)
    )

    # Market share calculations
    total_value = school_table["total_value"].sum()
    total_deals = school_table["deal_count"].sum()

    school_table["% of NIL Value"] = school_table["total_value"] / total_value
    school_table["% of Deals"] = school_table["deal_count"] / total_deals

    st.dataframe(
        school_table.style.format({
            "total_value": "${:,.0f}",
            "avg_value": "${:,.0f}",
            "median_value": "${:,.0f}",
            "% of NIL Value": "{:.1%}",
            "% of Deals": "{:.1%}",
        }),
        use_container_width=True
    )

st.caption(
    "Values are deduplicated per athlete and unique NIL deal before aggregation. "
    "This prevents inflation from repeated reporting."
)


# ============================================================
# STRATEGIC TAKEAWAYS
# ============================================================
st.header("📌 Takeaways")

st.write("""
• NIL deal **volume** spans many divisions, but value is highly concentrated  
• Powerhouse schools anchor high-dollar deals, while long-tail athletes drive scale  
• Division mix suggests opportunity for **lower-cost brand entry outside top football programs**  
""")

st.success("Dashboard Build Complete")
