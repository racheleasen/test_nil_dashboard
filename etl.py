#!/usr/bin/env python3
"""
etl_college_nil_mobile.py
===========================================
End-to-end ETL for:
- IPEDS institution metadata
- NIL state-level economics (manual export)
- EADA athletics economics (wide-format)
- FCC mobile coverage (state-level geometry from area file)

Outputs clean, consistent processed CSVs for downstream modeling.
"""

import os
import time
from typing import Dict, Any, List, Optional

import requests
import pandas as pd


# ============================================================
# CONFIG
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")

IPEDS_YEAR = 2022
URBAN_BASE = "https://educationdata.urban.org/api/v1"


# ============================================================
# UTILITIES
# ============================================================

def ensure_dirs() -> None:
    """Ensure required data directories exist."""
    for d in (DATA_DIR, RAW_DIR, PROCESSED_DIR):
        os.makedirs(d, exist_ok=True)


def fetch_urban_endpoint(url: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Paginated request to Urban Institute Education Data API.
    Returns a unified DataFrame of all pages.
    """
    print(f"[URBAN] Fetching {url} ...")
    rows: List[Dict[str, Any]] = []
    session = requests.Session()
    next_url = url

    while next_url:
        resp = session.get(next_url, params=params, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"Urban API error {resp.status_code}: {resp.text[:500]}")

        data = resp.json()
        rows.extend(data.get("results", []))
        next_url = data.get("next")
        time.sleep(0.25)  # politeness throttle

    print(f"[URBAN] Retrieved {len(rows)} rows.")
    return pd.DataFrame(rows)


# ============================================================
# IPEDS PIPELINE
# ============================================================

def download_ipeds_directory(year: int = IPEDS_YEAR) -> str:
    """Pull IPEDS directory for the specified year and cache locally."""
    out_path = os.path.join(RAW_DIR, f"ipeds_directory_{year}.csv")
    if os.path.exists(out_path):
        print("[SKIP] IPEDS directory already present.")
        return out_path

    url = f"{URBAN_BASE}/college-university/ipeds/directory/{year}/"
    df = fetch_urban_endpoint(url)
    df.columns = [c.lower() for c in df.columns]
    df.to_csv(out_path, index=False)

    print(f"[OK] Saved IPEDS directory → {out_path}")
    return out_path


def build_ipeds_institution_demographics(year: int = IPEDS_YEAR) -> str:
    """
    Expanded IPEDS institution metadata extract.
    Pulls dozens of useful fields for analysis:
      - Identity: unitid, school_name
      - Geography: city, county_name, county_fips, state_abbr, latitude, longitude
      - Urbanicity: urban_centric_locale
      - Control & sector
      - Level: undergrad/grad offerings, degree granting, institution_level
      - Size: inst_size
      - System: inst_system_flag, inst_system_name
      - CBSA: cbsa, cbsa_type, csa
      - Carnegie 2021 classifications: cc_basic_2021, cc_instruc_undergrad_2021, ...
    """
    dir_path = download_ipeds_directory(year)
    df = pd.read_csv(dir_path)
    df.columns = [c.lower().strip() for c in df.columns]

    # ------------------------------------------------------------
    # STEP 1 — Define expanded schema to extract
    # ------------------------------------------------------------
    expanded_cols = [
        # Identity
        "unitid", "inst_name", "institution_name",

        # Geography
        "state_abbr", "city", "county_name", "county_fips",
        "latitude", "longitude",

        # Urbanicity
        "urban_centric_locale",

        # Sector / control / level
        "inst_control", "sector", "institution_level",
        "degree_granting", "offering_undergrad", "offering_grad",

        # Size & admin
        "inst_size", "inst_system_flag", "inst_system_name",

        # CBSA regioning
        "cbsa", "cbsa_type", "csa",

        # Carnegie 2021 classifications (very important for segmentation)
        "cc_basic_2021",
        "cc_instruc_undergrad_2021",
        "cc_instruc_grad_2021",
        "cc_undergrad_2021",
        "cc_enroll_2021",
        "cc_size_setting_2021",

        # Nice-to-have URLs
        "url_school", "url_application", "url_fin_aid",
    ]

    # Keep only columns that actually exist
    keep_cols = [c for c in expanded_cols if c in df.columns]
    inst = df[keep_cols].copy()

    # ------------------------------------------------------------
    # STEP 2 — Normalize school_name
    # ------------------------------------------------------------
    if "inst_name" in inst.columns:
        inst = inst.rename(columns={"inst_name": "school_name"})
    elif "institution_name" in inst.columns:
        inst = inst.rename(columns={"institution_name": "school_name"})
    else:
        name_candidate = next((c for c in inst.columns if "name" in c), None)
        inst["school_name"] = inst[name_candidate] if name_candidate else pd.NA

    # ------------------------------------------------------------
    # STEP 3 — Normalize state abbreviation
    # ------------------------------------------------------------
    if "state_abbr" in inst.columns:
        inst["state_abbr"] = (
            inst["state_abbr"]
            .astype(str)
            .str.upper()
            .str.strip()
        )
    else:
        inst["state_abbr"] = pd.NA

    # ------------------------------------------------------------
    # STEP 4 — Clean up FIPS codes
    # ------------------------------------------------------------
    if "county_fips" in inst.columns:
        inst["county_fips"] = (
            inst["county_fips"]
            .astype(str)
            .str.zfill(5)
            .replace("00000", pd.NA)
        )

    # ------------------------------------------------------------
    # STEP 5 — Drop duplicates
    # ------------------------------------------------------------
    inst = inst.drop_duplicates(subset="unitid")

    # ------------------------------------------------------------
    # STEP 6 — Save
    # ------------------------------------------------------------
    out_path = os.path.join(PROCESSED_DIR, "ipeds_institution_demographics.csv")
    inst.to_csv(out_path, index=False)
    print(f"[OK] Saved expanded IPEDS institution demographics → {out_path}")

    return out_path


# ============================================================
# NIL PIPELINE
# ============================================================

def process_nil_raw() -> Optional[str]:
    """
    Process NIL state-level Looker export (manual download).

    Expects some combination of:
      - state_abbr / state / state_code
      - total_nil_dollars / total_value / sum_amount
      - nil_deals / num_deals / count_deals
    """
    in_path = os.path.join(RAW_DIR, "nil_state_export.csv")
    if not os.path.exists(in_path):
        print("[INFO] NIL export not found. Skip.")
        return None

    df = pd.read_csv(in_path)
    df.columns = [c.lower().strip() for c in df.columns]

    state_col = next((c for c in ["state_abbr", "state", "state_code"] if c in df.columns), None)
    money_col = next((c for c in ["total_nil_dollars", "total_value", "sum_amount"] if c in df.columns), None)
    deals_col = next((c for c in ["nil_deals", "num_deals", "count_deals"] if c in df.columns), None)

    if not all([state_col, money_col, deals_col]):
        print("[WARN] NIL file missing required columns.")
        return None

    out = df[[state_col, money_col, deals_col]].rename(columns={
        state_col: "state_abbr",
        money_col: "total_nil_dollars",
        deals_col: "nil_deals"
    })

    out["state_abbr"] = out["state_abbr"].astype(str).str.upper().str.strip()
    out["total_nil_dollars"] = pd.to_numeric(out["total_nil_dollars"], errors="coerce").fillna(0)
    out["nil_deals"] = pd.to_numeric(out["nil_deals"], errors="coerce").fillna(0)

    out_path = os.path.join(PROCESSED_DIR, "nil_state_level.csv")
    out.to_csv(out_path, index=False)

    print(f"[OK] Saved NIL state-level → {out_path}")
    return out_path


# ============================================================
# FCC PIPELINE (STATE-LEVEL COVERAGE)
# ============================================================

STATE_MAP = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY"
}


def process_fcc_mobile_raw() -> Optional[str]:
    """
    Processes FCC 'area' coverage geometry into state-level coverage metrics.

    Input (your example):
      - area_data_type
      - geography_type (National / State / County etc.)
      - geography_id
      - geography_desc (e.g., 'Alabama')
      - total_area
      - mobilebb_4g_area_st_pct
      - mobilebb_5g_spd1_area_st_pct
      etc.

    Output: processed/fcc_mobile_coverage_by_area.csv with
      - state_abbr
      - pct4g_st
      - pct5g_low_st
      - coverage_mobile_score
    """
    in_path = os.path.join(RAW_DIR, "fcc_mobile_county.csv")
    if not os.path.exists(in_path):
        print("[INFO] FCC file missing.")
        return None

    df = pd.read_csv(in_path)
    df.columns = [c.lower().strip() for c in df.columns]

    required = ["geography_type", "geography_desc", "total_area"]
    if not all(c in df.columns for c in required):
        print("[WARN] FCC file did not contain expected geometry columns.")
        return None

    # Keep only state rows
    state_df = df[df["geography_type"].str.lower() == "state"].copy()

    # Map full state name → postal abbreviation
    state_df["state_abbr"] = state_df["geography_desc"].map(STATE_MAP)
    state_df["state_abbr"] = state_df["state_abbr"].astype(str).str.upper().str.strip()

    # Coverage cols of interest
    state_df["pct4g_st"] = pd.to_numeric(state_df.get("mobilebb_4g_area_st_pct", 0), errors="coerce").fillna(0)
    state_df["pct5g_low_st"] = pd.to_numeric(state_df.get("mobilebb_5g_spd1_area_st_pct", 0), errors="coerce").fillna(0)

    # Simple unified coverage score (can tweak weights later)
    state_df["coverage_mobile_score"] = 0.6 * state_df["pct4g_st"] + 0.4 * state_df["pct5g_low_st"]

    out = state_df[["state_abbr", "pct4g_st", "pct5g_low_st", "coverage_mobile_score"]]

    out_path = os.path.join(PROCESSED_DIR, "fcc_mobile_coverage_by_area.csv")
    out.to_csv(out_path, index=False)

    print(f"[OK] Saved FCC state coverage → {out_path}")
    return out_path


# ============================================================
# EADA PIPELINE
# ============================================================

def process_eada_raw() -> Optional[str]:
    """
    Processes wide-format EADA_2024.csv into robust athletics summary.

    Output columns:
      - unitid
      - school_name
      - state_abbr
      - total_revenue_all_sports
      - total_expense_all_sports
      - net_athletics_margin
      - athletics_margin_pct
      - football_revenue_share
      - mbb_revenue_share
      - wbb_revenue_share
      - non_revenue_sports_ratio
      - head_coach_salary_total
      - recruiting_budget_total
      - revenue_per_athlete
      - expense_per_athlete
      - recruiting_intensity
    """
    in_path = os.path.join(RAW_DIR, "eada_2024.csv")
    if not os.path.exists(in_path):
        print("[INFO] EADA_2024.csv missing.")
        return None

    df = pd.read_csv(in_path)
    df.columns = [c.lower().strip() for c in df.columns]

    unit = "unitid" if "unitid" in df.columns else None
    inst = next((c for c in ["institution_name", "inst_name", "school_name"] if c in df.columns), None)
    state = next((c for c in ["state_abbr", "state", "state_cd"] if c in df.columns), None)

    if not all([unit, inst, state]):
        print("[WARN] EADA missing identity fields. Columns:", df.columns.tolist())
        return None

    # Numeric columns (revenue/expense/salary/recruit)
    numeric_cols = [c for c in df.columns if any(k in c for k in ["revenue", "expense", "salary", "recruit"])]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    # Aggregate total revenue / expense
    rev_cols = [c for c in df.columns if c.startswith("total_revenue_all_")]
    exp_cols = [c for c in df.columns if c.startswith("total_expense_all_")]

    df["total_revenue_all_sports"] = df[rev_cols].sum(axis=1)
    df["total_expense_all_sports"] = df[exp_cols].sum(axis=1)
    df["net_athletics_margin"] = df["total_revenue_all_sports"] - df["total_expense_all_sports"]
    df["athletics_margin_pct"] = df["net_athletics_margin"] / df["total_revenue_all_sports"].replace(0, pd.NA)

    # Helper to find sport revenue columns
    def find_col(prefix: str) -> Optional[str]:
        return next((c for c in df.columns if prefix in c), None)

    football_rev = find_col("total_revenue_all_football")
    mbb_rev = find_col("total_revenue_all_bskball")
    wbb_rev = find_col("total_revenue_all_wbskball") or find_col("total_revenue_all_softball")

    df["football_revenue_share"] = df[football_rev] / df["total_revenue_all_sports"] if football_rev else 0
    df["mbb_revenue_share"] = df[mbb_rev] / df["total_revenue_all_sports"] if mbb_rev else 0
    df["wbb_revenue_share"] = df[wbb_rev] / df["total_revenue_all_sports"] if wbb_rev else 0

    # Non-revenue sports ratio
    df["non_revenue_sports_ratio"] = (
        df["total_revenue_all_sports"] -
        df[[c for c in [football_rev, mbb_rev] if c]].sum(axis=1)
    ) / df["total_revenue_all_sports"].replace(0, pd.NA)

    # Coach + recruiting
    df["head_coach_salary_total"] = df[[c for c in df.columns if "hdcoach_salary" in c]].sum(axis=1)
    df["recruiting_budget_total"] = df[[c for c in df.columns if "recruitexp" in c]].sum(axis=1)

    # Per-athlete economics
    athlete_col = next((c for c in ["eftotalcount", "total_athletes"] if c in df.columns), None)
    if athlete_col:
        denom = df[athlete_col].replace(0, pd.NA)
        df["revenue_per_athlete"] = df["total_revenue_all_sports"] / denom
        df["expense_per_athlete"] = df["total_expense_all_sports"] / denom
        df["recruiting_intensity"] = df["recruiting_budget_total"] / denom
    else:
        df["revenue_per_athlete"] = pd.NA
        df["expense_per_athlete"] = pd.NA
        df["recruiting_intensity"] = pd.NA

    # Normalize identity columns
    df = df.rename(columns={unit: "unitid", inst: "school_name", state: "state_abbr"})
    df["state_abbr"] = df["state_abbr"].astype(str).str.upper().str.strip()

    # Guarantee school_name column exists
    if "school_name" not in df.columns:
        name_candidate = next((c for c in df.columns if "name" in c), None)
        if name_candidate:
            df["school_name"] = df[name_candidate]
        else:
            df["school_name"] = pd.NA

    out_cols = [
        "unitid", "school_name", "state_abbr",
        "total_revenue_all_sports", "total_expense_all_sports",
        "net_athletics_margin", "athletics_margin_pct",
        "football_revenue_share", "mbb_revenue_share", "wbb_revenue_share",
        "non_revenue_sports_ratio",
        "head_coach_salary_total", "recruiting_budget_total",
        "revenue_per_athlete", "expense_per_athlete", "recruiting_intensity"
    ]
    out = df[out_cols]

    out_path = os.path.join(PROCESSED_DIR, "eada_athletics_by_school.csv")
    out.to_csv(out_path, index=False)

    print(f"[OK] Saved EADA summary → {out_path}")
    return out_path


# ============================================================
# JOIN VALIDATION
# ============================================================

def test_joins() -> None:
    """Validate IPEDS + EADA + FCC merges cleanly and show basic diagnostics."""
    ipeds_path = os.path.join(PROCESSED_DIR, "ipeds_institution_demographics.csv")
    eada_path = os.path.join(PROCESSED_DIR, "eada_athletics_by_school.csv")
    fcc_path = os.path.join(PROCESSED_DIR, "fcc_mobile_coverage_by_area.csv")

    if not (os.path.exists(ipeds_path) and os.path.exists(eada_path) and os.path.exists(fcc_path)):
        print("[WARN] One or more processed files missing. Skipping join tests.")
        return

    ipeds = pd.read_csv(ipeds_path)
    eada = pd.read_csv(eada_path)
    fcc = pd.read_csv(fcc_path)

    print("\n--- JOIN TESTS ---")
    print("IPEDS shape:", ipeds.shape)
    print("EADA  shape:", eada.shape)
    print("FCC   shape:", fcc.shape)

    # IPEDS × EADA on unitid → expect school_name_x (IPEDS) + school_name_y (EADA)
    j1 = ipeds.merge(eada, on="unitid", how="left", suffixes=("_ipeds", "_eada"))
    print("IPEDS × EADA shape:", j1.shape)

    eada_name_col = "school_name_eada" if "school_name_eada" in j1.columns else None
    if eada_name_col:
        eada_match_rate = j1[eada_name_col].notna().mean()
        print(f"EADA match rate (rows with athletics data): {eada_match_rate:.3f}")
    else:
        print("EADA match rate: could not find school_name_eada column")

    # IPEDS × FCC on state_abbr
    if "state_abbr" not in ipeds.columns:
        print("[WARN] IPEDS missing state_abbr; cannot join to FCC.")
        return

    j2 = ipeds.merge(fcc, on="state_abbr", how="left")
    print("IPEDS × FCC shape:", j2.shape)
    fcc_match_rate = j2["coverage_mobile_score"].notna().mean()
    print(f"FCC coverage match rate (by state): {fcc_match_rate:.3f}")

    # Unified: IPEDS × EADA × FCC
    # j1 has state_abbr_ipeds / state_abbr_eada? Let's inspect:
    state_cols = [c for c in j1.columns if "state_abbr" in c.lower()]
    print("State columns in IPEDS×EADA:", state_cols)

    # Prefer IPEDS state_abbr if available
    left_state_col = None
    for cand in ["state_abbr_ipeds", "state_abbr"]:
        if cand in j1.columns:
            left_state_col = cand
            break

    if left_state_col is None:
        print("[WARN] No state_abbr column in IPEDS×EADA join; skipping unified join.")
        return

    j3 = j1.merge(fcc, left_on=left_state_col, right_on="state_abbr", how="left")
    print("Unified dataset shape (IPEDS × EADA × FCC):", j3.shape)
    unified_fcc_match = j3["coverage_mobile_score"].notna().mean()
    print(f"Unified FCC match rate: {unified_fcc_match:.3f}")

    print("\nSample unified rows:")
    print(j3.head(5))


# ============================================================
# MAIN DRIVER
# ============================================================

def main() -> None:
    ensure_dirs()

    print("\n=== STEP 1: IPEDS ===")
    ipeds_path = build_ipeds_institution_demographics(IPEDS_YEAR)

    print("\n=== STEP 2: NIL ===")
    nil_path = process_nil_raw()

    print("\n=== STEP 3: EADA ===")
    eada_path = process_eada_raw()

    print("\n=== STEP 4: FCC Mobile ===")
    fcc_path = process_fcc_mobile_raw()

    print("\n=== JOIN VALIDATION ===")
    test_joins()

    print("\n=== SUMMARY ===")
    print("IPEDS →", ipeds_path)
    print("NIL   →", nil_path)
    print("EADA  →", eada_path)
    print("FCC   →", fcc_path)


if __name__ == "__main__":
    main()
