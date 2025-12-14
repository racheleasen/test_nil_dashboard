#!/usr/bin/env python3
"""
NIL Collegiate Dashboard (2022–2025)
Executive Theme — Streamlit + Altair
"""

import pandas as pd
import streamlit as st
import altair as alt

# --------------------------------------------
# THEME SETUP (Altair 5.x)
# --------------------------------------------
def modern_vibe():
    return {
        "config": {
            "background": "#0f172a",
            "title": {"fontSize": 18, "font": "Inter", "anchor": "start", "color": "#f1f5f9"},
            "axis": {
                "labelFontSize": 12,
                "labelColor": "#e2e8f0",
                "titleFontSize": 13,
                "titleColor": "#f8fafc",
                "gridColor": "#334155",
                "domainColor": "#475569"
            },
            "legend": {
                "labelColor": "#e2e8f0",
                "titleColor": "#f8fafc",
                "orient": "bottom"
            },
            "view": {"stroke": "transparent"}
        }
    }

alt.themes.register("modern_vibe", modern_vibe)
alt.themes.enable("modern_vibe")

# --------------------------------------------
# PAGE CONFIGURATION
# --------------------------------------------
st.set_page_config(page_title="NIL Collegiate Dashboard", layout="wide")
st.markdown("""
    <style>
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        background-color: #0f172a;
        color: #e2e8f0;
    }
    h1, h2, h3 {
        color: #f8fafc !important;
    }
    .block-container {
        padding-top: 2rem;
    }
    </style>
""", unsafe_allow_html=True)

# --------------------------------------------
# LOAD DATA
# --------------------------------------------
df = pd.read_csv("data/processed/on3_nil_deals_all.csv")
df_dedupe = pd.read_csv("data/processed/on3_nil_athlete_values.csv")

df["deal_date"] = pd.to_datetime(df["deal_date"], errors="coerce")
df = df[df["deal_date"].dt.year.between(2022, 2025)]
df["sport_name"] = df["sport_name"].fillna("Unknown")
df["player_state"] = df["player_state"].fillna("Unknown")

col1, spacer, col2 = st.columns([2, 0.1, 1])

with col1:
    st.title("NIL Collegiate Dashboard")
    st.caption("Analysis of NIL activity and market concentration, 2022–2025")

with col2:
    st.title("Filters")

# Intro + Key Observations
col1, spacer, col2 = st.columns([2, 0.1, 1])

with col1:
    st.markdown("""
    This dashboard summarizes Name, Image, and Likeness (NIL) activity from 2022–2025.
    It highlights where activity concentrates, how value is distributed, and which institutions,
    athletes, and categories materially influence the market.

    *Source: [On3 NIL Valuations](https://www.on3.com/nil/deals/)*
    """)

    st.subheader("Key Observations")
    st.markdown("""
    - NIL deal volume is broadly distributed, but reported value is concentrated among a limited number of institutions.  
    - A small percentage of athletes receive the majority of reported NIL dollars.  
    - Several sports and schools show high engagement despite modest deal values, suggesting alternative partnership potential.
    """)

with col2:
    selected_school = st.multiselect("School", sorted(df["team_committed"].dropna().unique()))
    selected_sports = st.multiselect("Sport", sorted(df["sport_name"].dropna().unique()))
    year_range = st.slider("Year Range", 2022, 2025, (2022, 2025))

# Apply filters
filtered_df = df.copy()
filtered_dedupe = df_dedupe.copy()

if selected_school:
    filtered_df = filtered_df[filtered_df["team_committed"].isin(selected_school)]
    filtered_dedupe = filtered_dedupe[filtered_dedupe["team_committed"].isin(selected_school)]
if selected_sports:
    filtered_df = filtered_df[filtered_df["sport_name"].isin(selected_sports)]
    filtered_dedupe = filtered_dedupe[filtered_dedupe["sport_name"].isin(selected_sports)]

filtered_df = filtered_df[
    filtered_df["deal_date"].dt.year.between(year_range[0], year_range[1])
]

st.markdown("---")

# --------------------------------------------
# KPIs — MARKET OVERVIEW
# --------------------------------------------
col1, spacer, col2 = st.columns([1, 0.01, 1])

with col1:
    st.title("Market Overview")
    st.caption("Key indicators summarizing NIL activity volume, school and athlete participation, and deal value disclosures.")

    k1, k2 = st.columns(2)
    k3, k4 = st.columns(2)
    k5, k6 = st.columns(2)

    reported_deals = filtered_df["deal_amount"].notnull().sum()
    total_deals = filtered_df["deal_key"].nunique()
    share_reported = reported_deals / total_deals if total_deals else 0

    reported_athletes = filtered_df[filtered_df["deal_amount"].notnull()]["player_key"].nunique()
    total_athletes = filtered_df["player_key"].nunique()

    k1.metric("Total Deals", f"{len(filtered_df):,}")
    k2.metric("Schools Represented", filtered_df["team_committed"].nunique())
    k3.metric("Athletes Represented", total_athletes)
    k4.metric("Athletes with Disclosed NIL Values", f"{reported_athletes / total_athletes:.1%}" if total_athletes else "0.0%")
    k5.metric("Average Disclosed Deal Value", f"${filtered_df['deal_amount'].mean():,.0f}")
    k6.metric("Deals with Disclosed Values", f"{share_reported:.1%}")

# --------------------------------------------
# TIME SERIES — DEALS OVER TIME
# --------------------------------------------
filtered_df["deal_month"] = filtered_df["deal_date"].dt.to_period("M").dt.to_timestamp()
time_series = filtered_df.groupby("deal_month").size().reset_index(name="deals")

time_line = alt.Chart(time_series).mark_line(point=True, strokeWidth=3, color="#ef4444").encode(
    x="deal_month:T", y="deals:Q", tooltip=["deal_month", "deals"]
)

with col2:
    st.subheader("NIL Deals Over Time")
    st.caption("Monthly trend of NIL deal activity across all athletes and institutions from 2022 to 2025.")
    st.altair_chart(time_line.properties(height=350), use_container_width=True)

st.markdown("---")

# --------------------------------------------
# TOP SCHOOLS + TOP BRANDS
# --------------------------------------------
col1, spacer, col2 = st.columns([1, 0.1, 1])

school_summary = (
    filtered_df.groupby("team_committed")
    .agg(deals=("deal_key", "count"), athletes=("player_key", "nunique"))
    .sort_values("deals", ascending=False)
    .head(10)
    .reset_index()
)

school_bars = alt.Chart(school_summary).mark_bar(color="#2563eb").encode(
    x="deals:Q",
    y=alt.Y("team_committed:N", sort="-x"),
    tooltip=["team_committed", "deals", "athletes"]
)

with col1:
    st.subheader("Top NIL Schools")
    st.caption("The 10 most active schools based on total NIL deal volume, regardless of value.")
    st.altair_chart(school_bars.properties(height=350), use_container_width=True)

brand_volume = (
    filtered_df.groupby("company_name")
    .size()
    .reset_index(name="deal_count")
    .sort_values("deal_count", ascending=False)
    .head(10)
)

brand_bars = alt.Chart(brand_volume).mark_bar(color="#6b7280").encode(
    x=alt.X("deal_count:Q", title="Number of NIL Deals"),
    y=alt.Y("company_name:N", sort="-x", title="Brand"),
    tooltip=["company_name", "deal_count"]
)

with col2:
    st.subheader("Most Active Brands (Deal Volume)")
    st.caption("Brands with the highest number of NIL deals signed, highlighting frequent sponsors and activators.")
    st.altair_chart(brand_bars.properties(height=350), use_container_width=True)

st.markdown("---")

# --------------------------------------------
# TOP ATHLETES — VOLUME VS VALUE
# --------------------------------------------

col1, spacer, col2 = st.columns([1, 0.1, 1])

athlete_volume = (
    filtered_df.groupby(["player_key", "player_name"])
    .size()
    .reset_index(name="deal_count")
    .sort_values("deal_count", ascending=False)
    .head(10)
)

volume_bars = alt.Chart(athlete_volume).mark_bar(color="#7c3aed").encode(
    x="deal_count:Q",
    y=alt.Y("player_name:N", sort="-x"),
    tooltip=["player_name", "deal_count"]
)

with col1:
    st.subheader("Top 10 NIL Deals (Volume)")
    st.caption("Athletes with the highest number of reported NIL deals across all categories.")
    st.altair_chart(volume_bars.properties(height=350), use_container_width=True)

df_money = filtered_df[filtered_df["deal_amount"].notnull()]
athlete_value = (
    df_money.groupby(["player_key", "player_name"])
    .agg(total_value=("deal_amount", "mean"), deal_count=("deal_amount", "count"), avg_value=("deal_amount", "mean"))
    .reset_index()
    .sort_values("total_value", ascending=False)
    .head(10)
)

value_bars = alt.Chart(athlete_value).mark_bar(color="#2563eb").encode(
    x=alt.X("total_value:Q", axis=alt.Axis(format="~s")),
    y=alt.Y("player_name:N", sort="-x"),
    tooltip=[
        "player_name",
        alt.Tooltip("total_value:Q", format="$,.0f"),
        "deal_count",
        alt.Tooltip("avg_value:Q", format="$,.0f")
    ]
)

with col2:
    st.subheader("Highest NIL Value (Reported $)")
    st.caption("Athletes with the highest reported NIL value, based on average or total deal amounts disclosed.")
    st.altair_chart(value_bars.properties(height=350), use_container_width=True)

st.markdown("---")

# --------------------------------------------
# SCHOOL-LEVEL SUMMARY TABLE
# --------------------------------------------
st.header("School-Level NIL Summary")
st.caption("Aggregate school-level NIL totals for athletes with disclosed deal values, including market share and median deal size.")

school_money = (
    filtered_dedupe
    .loc[filtered_dedupe["deal_value"].notnull()]
    .groupby(["team_committed", "player_key"], as_index=False)
    .agg(deal_value=("deal_value", "max"))
)

if school_money.empty:
    st.warning("No reported NIL values available.")
else:
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

st.markdown("---")

st.caption(
    "Values are deduplicated per athlete and unique NIL deal before aggregation. "
    "This prevents inflation from repeated reporting."
)

st.success("Dashboard Build Complete")
