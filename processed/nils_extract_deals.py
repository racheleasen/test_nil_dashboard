#!/usr/bin/env python3
"""
FULL EXTRACTION VERSION — PULL ALL NIL DEALS FROM ON3 API

This script:
    ✔ Fetches all pages from the public On3 NIL API
    ✔ Flattens each NIL deal using the corrected schema
    ✔ Saves full dataset to data/processed/on3_nil_deals_all.csv
    ✔ Prints debug info for page 1 (first JSON + null summary)

"""

import os
import requests
import pandas as pd
import time
from pprint import pprint

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
BASE_URL = "https://api.on3.com/public/v2/deals?page={page}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
}

SLEEP = 0.35   # Do not hammer API

OUTPUT_DIR = "data/processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "on3_nil_deals_all.csv")


# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------

def fetch_page(page: int):
    """Fetch a given page from the On3 NIL API."""
    url = BASE_URL.format(page=page)
    print(f"[API] Fetching page {page}… {url}")
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def flatten_deal(d):
    """Correct flatten for NIL deal JSON based on real On3 schema."""

    person  = d.get("person") or {}
    rating  = d.get("rating") or {}
    roster  = d.get("rosterRating") or {}
    status  = d.get("status") or {}
    detail  = d.get("detail") or {}
    company = d.get("company") or {}

    sport_info = (rating.get("sport") 
              or person.get("defaultSport") 
              or {})

    committed   = status.get("committedAsset") or {}
    transferred = status.get("transferredAsset") or {}

    state_info    = person.get("state") or {}
    hometown_info = person.get("hometown") or {}
    position_info = person.get("position") or {}

    return {
        # Deal
        "deal_key": d.get("key"),
        "deal_date": d.get("date"),
        "deal_amount": d.get("nilValue"),   # Correct NIL valuation
        "nil_status": d.get("nilStatus"),
        "verified": d.get("verified"),
        "source_url": d.get("sourceUrl"),
        "type": d.get("type"),

        # Player info
        "player_key": person.get("key"),
        "first_name": person.get("firstName"),
        "last_name": person.get("lastName"),
        "player_name": person.get("fullName"),
        "player_slug": person.get("slug"),
        "player_position": position_info.get("abbr"),
        "player_height": person.get("height"),
        "player_weight": person.get("weight"),
        "player_class_year": person.get("classYear"),
        "player_division": person.get("division"),
        "player_state": state_info.get("abbr"),
        "player_hometown": hometown_info.get("abbr"),
        "sport_abbr": sport_info.get("abbr"),
        "sport_name": sport_info.get("name"),

        # Company
        "company_key": company.get("key"),
        "company_name": company.get("name"),

        # Rating data
        "rating": rating.get("rating"),
        "stars": rating.get("stars"),
        "national_rank": rating.get("nationalRank"),
        "position_rank": rating.get("positionRank"),
        "state_rank": rating.get("stateRank"),

        # Consensus fields
        "consensus_rating": rating.get("consensusRating"),
        "consensus_stars": rating.get("consensusStars"),
        "consensus_national_rank": rating.get("consensusNationalRank"),
        "consensus_position_rank": rating.get("consensusPositionRank"),
        "consensus_state_rank": rating.get("consensusStateRank"),

        # Roster data
        "roster_rating": roster.get("rating"),
        "roster_stars": roster.get("stars"),
        "roster_national_rank": roster.get("nationalRank"),

        # Status / School
        "status_type": status.get("type"),
        "status_date": status.get("date"),
        "team_committed": committed.get("name"),
        "team_transferred_from": transferred.get("name"),
        "school_state": committed.get("stateAbbr"),

        # Article / detail
        "headline": detail.get("title"),
        "article_slug": detail.get("slug"),
        "article_url": detail.get("fullUrl"),
        "article_date": detail.get("datePublishedGmt"),
    }


# ------------------------------------------------------------
# MAIN EXTRACTION
# ------------------------------------------------------------

def main():
    print("\n=========== STARTING NIL EXTRACTION ===========\n")

    # -------------------------------
    # 1. Fetch first page to get metadata
    # -------------------------------
    first = fetch_page(1)

    pagination = first.get("pagination", {})
    page_count = pagination.get("pageCount")
    total_rows = pagination.get("count")

    print("\n=== PAGINATION META ===")
    pprint(pagination)

    print(f"\n[INFO] Total pages: {page_count}")
    print(f"[INFO] Total expected deals: {total_rows}\n")

    # -------------------------------
    # 2. Debug: show raw JSON for first item
    # -------------------------------
    deals = first.get("list", [])
    print("\n=== RAW FIRST JSON ITEM ===")
    pprint(deals[0])

    # -------------------------------
    # 3. Build list of all rows (flatten)
    # -------------------------------
    all_rows = []

    print("\n[PROCESS] Flattening page 1…")
    for d in deals:
        all_rows.append(flatten_deal(d))

    # -------------------------------
    # 4. Loop through remaining pages
    # -------------------------------
    for page in range(2, page_count + 1):
        time.sleep(SLEEP)
        try:
            data = fetch_page(page)
            for d in data.get("list", []):
                all_rows.append(flatten_deal(d))
        except Exception as e:
            print(f"[WARN] Failed on page {page}: {e}")

    # -------------------------------
    # 5. Convert to DataFrame
    # -------------------------------
    df = pd.DataFrame(all_rows)
    print("\n=== FINAL DF SHAPE ===")
    print(df.shape)

    # -------------------------------
    # 6. Save to CSV
    # -------------------------------
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"\n[OK] Saved all NIL deals → {OUTPUT_PATH}\n")

    # -------------------------------
    # 7. Optional debug: null summary
    # -------------------------------
    print("\n=== NULL SUMMARY ===")
    print(df.isna().sum().sort_values(ascending=False))


if __name__ == "__main__":
    main()
