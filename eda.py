#!/usr/bin/env python3
"""
Time-Series Analysis of NIL Deals
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
df = pd.read_csv("data/processed/on3_nil_deals_all.csv")

# Convert dates
df["deal_date"] = pd.to_datetime(df["deal_date"], errors="coerce")
df["article_date"] = pd.to_datetime(df["article_date"], errors="coerce")

# Use deal_date primarily
df["date"] = df["deal_date"].fillna(df["article_date"])
df = df.dropna(subset=["date"])

# Monthly bucket
df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()

# ------------------------------------------------------------
# 1. Deal Count Over Time
# ------------------------------------------------------------
deal_volume = df.groupby("month").size().reset_index(name="deal_count")

plt.figure(figsize=(12, 4))
sns.lineplot(data=deal_volume, x="month", y="deal_count", marker="o")
plt.title("NIL Deal Volume Over Time")
plt.xlabel("Month")
plt.ylabel("Number of Deals")
plt.grid(True)
plt.tight_layout()
plt.show()

# ------------------------------------------------------------
# 2. NIL Value Over Time (Public Deals Only)
# ------------------------------------------------------------
df_value = df[df["deal_amount"].notna()]

value_trend = df_value.groupby("month")["deal_amount"].median().reset_index()

plt.figure(figsize=(12, 4))
sns.lineplot(data=value_trend, x="month", y="deal_amount", marker="o", color="green")
plt.title("Median NIL Valuation Over Time (Public Deals)")
plt.xlabel("Month")
plt.ylabel("Median NIL Value ($)")
plt.grid(True)
plt.tight_layout()
plt.show()

# ------------------------------------------------------------
# 3. Top Schools Over Time (Deal Count)
# ------------------------------------------------------------
top_schools = (
    df[df["team_committed"].notna()]
    .groupby("team_committed")
    .size()
    .sort_values(ascending=False)
    .head(10)
    .index
)

school_ts = (
    df[df["team_committed"].isin(top_schools)]
    .groupby(["month", "team_committed"])
    .size()
    .reset_index(name="deal_count")
)

plt.figure(figsize=(14, 6))
sns.lineplot(data=school_ts, x="month", y="deal_count", hue="team_committed")
plt.title("Top 10 NIL Schools – Deal Activity Over Time")
plt.xlabel("Month")
plt.ylabel("Deal Count")
plt.grid(True)
plt.legend(title="School")
plt.tight_layout()
plt.show()

# ------------------------------------------------------------
# 4. Brand NIL Activity Over Time
# ------------------------------------------------------------
top_brands = (
    df[df["company_name"].notna()]
    .groupby("company_name")
    .size()
    .sort_values(ascending=False)
    .head(10)
    .index
)

brand_ts = (
    df[df["company_name"].isin(top_brands)]
    .groupby(["month", "company_name"])
    .size()
    .reset_index(name="deal_count")
)

plt.figure(figsize=(14, 6))
sns.lineplot(data=brand_ts, x="month", y="deal_count", hue="company_name")
plt.title("Top NIL Brands – Deal Activity Over Time")
plt.xlabel("Month")
plt.ylabel("Deal Count")
plt.grid(True)
plt.legend(title="Brand")
plt.tight_layout()
plt.show()

# ------------------------------------------------------------
# 5. High School vs College NIL Trend
# ------------------------------------------------------------
df["nil_level"] = df["player_division"].apply(
    lambda x: "HighSchool" if x == "HighSchool" else "College"
)

level_ts = df.groupby(["month", "nil_level"]).size().reset_index(name="deal_count")

plt.figure(figsize=(12, 4))
sns.lineplot(data=level_ts, x="month", y="deal_count", hue="nil_level", marker="o")
plt.title("High School vs College NIL Deal Trend Over Time")
plt.xlabel("Month")
plt.ylabel("Deal Count")
plt.grid(True)
plt.tight_layout()
plt.show()
