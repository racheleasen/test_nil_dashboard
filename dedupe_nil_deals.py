#!/usr/bin/env python3
"""
DEDUPED NIL ATHLETE VALUE FACT TABLE
-----------------------------------

Creates an analysis-safe NIL valuation dataset by:

• ONE ROW per athlete per school
• MAX NIL valuation per athlete (economic signal)
• COUNT of unique deals per athlete (activity signal)
• Prevents inflation from repeated articles

INPUT:
  data/processed/on3_nil_deals_all.csv

OUTPUT:
  data/processed/on3_nil_athlete_values.csv
"""

import pandas as pd
import os

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
INPUT_PATH = "data/processed/on3_nil_deals_all.csv"
OUTPUT_PATH = "data/processed/on3_nil_athlete_values.csv"

os.makedirs("data/processed", exist_ok=True)

# ------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------
df = pd.read_csv(INPUT_PATH)

df["deal_date"] = pd.to_datetime(df["deal_date"], errors="coerce")

# ------------------------------------------------------------
# FILTER TO VALID ATHLETE ROWS
# ------------------------------------------------------------
value_df = df[
    df["player_key"].notnull() &
    df["team_committed"].notnull()
].copy()

print(f"[INFO] Raw athlete rows: {len(value_df):,}")

# ------------------------------------------------------------
# AGGREGATE TO ATHLETE FACTS
# ------------------------------------------------------------
athlete_fact = (
    value_df
    .groupby(
        ["player_key", "player_name", "team_committed"],
        as_index=False
    )
    .agg(
        # Economic signal
        deal_value=("deal_amount", "max"),

        # Activity signal
        deal_count=("deal_key", "nunique"),

        # Descriptors
        sport_name=("sport_name", "first"),
        player_state=("player_state", "first"),
        player_position=("player_position", "first"),
        player_class_year=("player_class_year", "first"),
    )
)

print(f"[INFO] Athlete rows created: {len(athlete_fact):,}")

# ------------------------------------------------------------
# SORT FOR ANALYSIS
# ------------------------------------------------------------
final_df = athlete_fact.sort_values(
    "deal_value",
    ascending=False,
    na_position="last"
)

# ------------------------------------------------------------
# SAVE OUTPUT
# ------------------------------------------------------------
final_df.to_csv(OUTPUT_PATH, index=False)

print(f"[OK] Saved athlete NIL fact table → {OUTPUT_PATH}")

# ------------------------------------------------------------
# SANITY CHECKS
# ------------------------------------------------------------
print("\n=== SANITY CHECKS ===")
print(
    "Total NIL Value (Deduped):",
    f"${final_df['deal_value'].dropna().sum():,.0f}"
)

print("Total Deals Represented:",
      int(final_df["deal_count"].sum()))

print("Unique Athletes:",
      final_df["player_key"].nunique())

print("Unique Schools:",
      final_df["team_committed"].nunique())
