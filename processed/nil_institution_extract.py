#!/usr/bin/env python3
"""
build_nil_institution_level.py
=====================================================
Transforms NIL *deal-level* data into *institution-level*
NIL economics for downstream EDA.

Steps:
  1. Load:
        - on3_nil_deals_all.csv     (deal-level NIL data)
        - ipeds_institution_demographics.csv (institution metadata)
  2. Detect the IPEDS institution name column dynamically.
  3. Clean & fuzzy-match NIL team names → IPEDS institutions.
  4. Aggregate to institution-level NIL metrics.
  5. Save:
        - nil_institution_level.csv
        - nil_team_to_unitid_mapping.csv (for QA / manual review)

Inputs (expected columns in NIL CSV):
  deal_key, deal_date, deal_amount, verified, nil_status, source_url, type,
  player_key, player_name, first_name, last_name, player_slug, player_position,
  player_height, player_weight, player_class_year, player_division,
  player_state, player_hometown, company_key, company_name, rating, stars,
  national_rank, position_rank, state_rank, roster_rating, roster_stars,
  roster_national_rank, status_type, status_date, team_committed,
  team_transferred_from, headline, article_slug, article_url, article_date

Output:
  data/processed/nil_institution_level.csv
"""

import os
import re
import pandas as pd
from rapidfuzz import process, fuzz

# ============================================================
# PATH CONFIG
# ============================================================

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE, "data", "processed")

NIL_INPUT = os.path.join(DATA_DIR, "on3_nil_deals_all.csv")
IPEDS_INPUT = os.path.join(DATA_DIR, "ipeds_institution_demographics.csv")
OUTPUT_INST = os.path.join(DATA_DIR, "nil_institution_level.csv")
OUTPUT_MAPPING = os.path.join(DATA_DIR, "nil_team_to_unitid_mapping.csv")

# ============================================================
# LOAD DATA
# ============================================================

print(f"[LOAD] NIL deals → {NIL_INPUT}")
nil = pd.read_csv(NIL_INPUT)
nil.columns = [c.lower().strip() for c in nil.columns]

print(f"[LOAD] IPEDS → {IPEDS_INPUT}")
ipeds = pd.read_csv(IPEDS_INPUT)
ipeds.columns = [c.lower().strip() for c in ipeds.columns]

# ============================================================
# DETECT INSTITUTION NAME COLUMN (IPEDs)
# ============================================================

possible_name_cols = [
    "school_name_ipeds",
    "school_name",
    "institution_name",
    "inst_name",
    "name",
]

name_col = next((c for c in possible_name_cols if c in ipeds.columns), None)

if name_col is None:
    raise ValueError(
        "Could not detect institution name column in IPEDS. "
        f"Available columns: {list(ipeds.columns)}"
    )

print(f"[INFO] Using IPEDS institution name column → {name_col}")


# ============================================================
# MONEY PARSER
# ============================================================

def parse_money(v):
    """
    Parse deal_amount into numeric dollars.

    Accepts:
      - "$50K", "$2M", "$1,200,000"
      - "Undisclosed" → 0
      - Already-numeric → float(v)
    """
    if pd.isna(v):
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)

    s = str(v).strip()
    if not s:
        return 0.0

    s = s.replace("$", "").replace(",", "").strip().lower()

    if s in {"undisclosed", "n/a", "na", "unknown"}:
        return 0.0

    # suffix formats: 1.2m, 500k
    if s.endswith("m"):
        try:
            return float(s[:-1]) * 1_000_000
        except ValueError:
            return 0.0
    if s.endswith("k"):
        try:
            return float(s[:-1]) * 1_000
        except ValueError:
            return 0.0

    # plain numeric
    try:
        return float(s)
    except ValueError:
        return 0.0


if "deal_amount" not in nil.columns:
    raise ValueError("NIL CSV must contain a 'deal_amount' column.")

nil["deal_amount_num"] = nil["deal_amount"].apply(parse_money)

# ============================================================
# EXTRACT TEAM NAME FOR MATCHING
# ============================================================

"""
The flattened CSV should include one or more of:
  - team_committed
  - team_transferred_from
  - team_name
We pick the best available column.
"""

possible_team_cols = [
    "team_committed",
    "team_transferred_from",
    "team_name",
]

team_col = next((c for c in possible_team_cols if c in nil.columns), None)
if team_col is None:
    raise ValueError(
        "Could not find any team_* column in NIL dataset. "
        f"Available columns: {list(nil.columns)}"
    )

print(f"[INFO] Using NIL team column → {team_col}")

nil["team_name_raw"] = nil[team_col].astype(str).str.strip()


# ============================================================
# NAME CLEANING FOR FUZZY MATCHING
# ============================================================

def clean_name(s: str) -> str:
    """Aggressive text clean for fuzzy matching."""
    if pd.isna(s):
        return ""
    s = str(s).lower()

    # Remove some common words/phrases
    s = re.sub(r"\buniversity\b", "", s)
    s = re.sub(r"\bcollege\b", "", s)
    s = re.sub(r"\bthe\b", "", s)
    s = re.sub(r"\bmen\b|\bwomen\b", "", s)
    s = re.sub(r"\bfb\b|\bncaa\b", "", s)
    s = re.sub(r"\bfootball\b", "", s)
    s = re.sub(r"\bstate university\b", "state", s)

    # Strip non-alphanumeric
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s)

    return s.strip()


nil["team_name_clean"] = nil["team_name_raw"].apply(clean_name)
ipeds["school_name_clean"] = ipeds[name_col].astype(str).apply(clean_name)

# Optional: filter out blank team names (e.g. high school, pro, or missing)
nil = nil[nil["team_name_clean"] != ""].copy()

# ============================================================
# FUZZY MATCH: NIL team_name_clean → IPEDS school_name_clean
# ============================================================

unique_teams = nil["team_name_clean"].dropna().unique()
ipeds_names = ipeds["school_name_clean"].tolist()

print(f"[FUZZY MATCH] Matching {len(unique_teams)} unique NIL teams to IPEDS…")

mapping_records = []
mapping = {}

for t in unique_teams:
    if not t:
        continue

    match, score, idx = process.extractOne(
        t,
        ipeds_names,
        scorer=fuzz.WRatio
    )

    # Strong-match threshold; tweak if needed
    if score >= 85:
        unitid = ipeds.iloc[idx]["unitid"]
        mapping[t] = unitid

        mapping_records.append(
            {
                "team_name_clean": t,
                "matched_school_clean": match,
                "match_score": score,
                "unitid": unitid,
                "iped_school_original": ipeds.iloc[idx][name_col],
            }
        )

mapping_df = pd.DataFrame(mapping_records).sort_values("match_score", ascending=False)
print(f"[MAP] Created mapping for {len(mapping_df)} team names with score ≥ 85.")

# Save mapping for QA / manual tweaks
mapping_df.to_csv(OUTPUT_MAPPING, index=False)
print(f"[OK] Saved NIL team → IPEDS mapping → {OUTPUT_MAPPING}")

# Apply mapping to NIL deals
nil["unitid"] = nil["team_name_clean"].map(mapping)

# ============================================================
# FILTER MAPPED DEALS
# ============================================================

mapped = nil.dropna(subset=["unitid"]).copy()
mapped["unitid"] = mapped["unitid"].astype(int)

print(f"[MAP] Successfully mapped {len(mapped)} deals to institutions.")

if mapped.empty:
    raise RuntimeError(
        "No NIL deals were successfully mapped to institutions. "
        "Check mapping thresholds / team name columns."
    )

# ============================================================
# ADD SOME HELPER FIELDS FOR AGGREGATION
# ============================================================

# Verified flag — support either 'verified' or 'verified_flag'
verified_col = None
for c in ["verified", "verified_flag"]:
    if c in mapped.columns:
        verified_col = c
        break

if verified_col is None:
    mapped["verified_bool"] = False
else:
    mapped["verified_bool"] = mapped[verified_col].astype(bool)

# Player stars weighting (NIL value × stars)
if "stars" in mapped.columns:
    mapped["stars"] = mapped["stars"].fillna(0)
else:
    mapped["stars"] = 0

mapped["stars_weighted_deal"] = mapped["stars"] * mapped["deal_amount_num"]

# ============================================================
# INSTITUTION-LEVEL AGGREGATION
# ============================================================

inst_nil = (
    mapped
    .groupby("unitid")
    .agg(
        nil_deal_count=("deal_key", "count"),
        nil_total_dollars=("deal_amount_num", "sum"),
        nil_avg_deal=("deal_amount_num", "mean"),
        nil_median_deal=("deal_amount_num", "median"),
        nil_verified_count=("verified_bool", "sum"),
        nil_distinct_players=("player_key", "nunique"),
        nil_distinct_companies=("company_key", "nunique"),
        nil_stars_sum=("stars", "sum"),
        nil_stars_weighted_total=("stars_weighted_deal", "sum"),
    )
    .reset_index()
)

# Stars-weighted average dollars per star (handle divide-by-zero)
inst_nil["nil_dollars_per_star"] = inst_nil.apply(
    lambda r: r["nil_stars_weighted_total"] / r["nil_stars_sum"]
    if r["nil_stars_sum"] > 0 else 0,
    axis=1
)

print("\n[PREVIEW] Institution-level NIL metrics:")
print(inst_nil.head())

# ============================================================
# SAVE OUTPUT
# ============================================================

inst_nil.to_csv(OUTPUT_INST, index=False)
print(f"\n[OK] Saved institution-level NIL metrics → {OUTPUT_INST}")
print(f"[DONE] Rows: {len(inst_nil)}")
