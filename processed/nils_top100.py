#!/usr/bin/env python3
"""
on3_nil_scraper_option_c.py
==================================================
Option C: Robust-ish NIL scraping from On3 NIL valuations rankings.

Strategy:
  - Hit the public NIL valuations rankings page:
      https://www.on3.com/nil/rankings/player/nil-valuations/
  - Parse the *rendered HTML* (players, teams, valuations) using BeautifulSoup.
  - Avoid any brittle JSON (__NEXT_DATA__) paths.

Outputs:
  - on3_nil_players_sample.csv   (player-level NIL data)
  - on3_nil_teams_sample.csv     (team-level NIL summary)
"""

import os
import re
import time
from typing import List, Dict, Any, Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)

ON3_BASE_URL = "https://www.on3.com"
RANKINGS_URL = f"{ON3_BASE_URL}/nil/rankings/player/nil-valuations/"

HEADERS = {
    # Behave like a normal browser
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0 Safari/537.36"
    )
}


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def fetch_html(url: str, sleep_sec: float = 1.0) -> str:
    """GET the page HTML with a polite delay."""
    print(f"[HTTP] GET {url}")
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    time.sleep(sleep_sec)
    return resp.text


def parse_money_str(s: str) -> Optional[float]:
    """
    Parse strings like "$5.3M", "$2M", "$850K" into numeric dollars.
    Returns None if parsing fails.
    """
    if not isinstance(s, str):
        return None
    s = s.strip()
    if not s.startswith("$"):
        return None
    # Remove `$` and commas
    core = s[1:].replace(",", "")
    m = re.match(r"^([\d\.]+)\s*([MK])?$", core, flags=re.IGNORECASE)
    if not m:
        return None
    value = float(m.group(1))
    suffix = m.group(2)
    if suffix is None:
        return value
    suffix = suffix.upper()
    if suffix == "M":
        return value * 1_000_000
    if suffix == "K":
        return value * 1_000
    return value


def class_contains(fragment: str):
    """Return a filter function for BeautifulSoup class-based matching."""
    def _matcher(classes):
        if not classes:
            return False
        if isinstance(classes, str):
            return fragment in classes
        return any(fragment in c for c in classes)
    return _matcher


# -------------------------------------------------------------------
# CORE PARSER
# -------------------------------------------------------------------

def parse_on3_nil_rankings_page(html: str) -> pd.DataFrame:
    """
    Parse the On3 NIL valuations rankings HTML into a player-level DataFrame.

    For each visible player row, attempts to extract:
      - rank
      - position
      - player_name
      - player_href (relative URL)
      - team_name   (from team avatar img alt, e.g. "Texas Longhorns Avatar")
      - nil_valuation_str  (e.g. "$5.3M")
      - nil_valuation_dollars (float)
    """
    soup = BeautifulSoup(html, "html.parser")

    # Each player row is rendered by a React component named something like:
    #   NilPlayerRankingItem_itemContainer__<hash>
    # We'll search for any div whose class contains that prefix.
    row_divs = soup.find_all("div", class_=class_contains("NilPlayerRankingItem_itemContainer"))
    print(f"[PARSE] Found {len(row_divs)} player rows (approx).")

    records: List[Dict[str, Any]] = []

    for row in row_divs:
        # -----------------------------
        # Player name + link
        # -----------------------------
        name_anchor = row.find("a", class_=class_contains("NilPlayerRankingItem_name"))
        if not name_anchor:
            # Skip weird rows (e.g., headers)
            continue
        player_name = name_anchor.get_text(strip=True)
        player_href = name_anchor.get("href", "")
        if player_href and player_href.startswith("/"):
            player_href = ON3_BASE_URL + player_href

        # -----------------------------
        # Position (e.g., QB, WR)
        # Often rendered as a short text element within the row.
        # We'll look for a short uppercase string near the top.
        # -----------------------------
        position = None
        # Try to find span or div with short uppercase code
        for elt in row.find_all(["span", "div"], recursive=True):
            txt = elt.get_text(strip=True)
            if 1 <= len(txt) <= 4 and txt.isupper() and txt.isalpha():
                position = txt
                break

        # -----------------------------
        # Team name:
        # Use non-default avatar image alt text (e.g. "Texas Longhorns Avatar")
        # -----------------------------
        team_name = None
        for img in row.find_all("img"):
            alt = img.get("alt", "") or ""
            if not alt:
                continue
            if "Default Avatar" in alt:
                continue
            # Many are like "texas longhorns Avatar" or "Texas Longhorns"
            # Normalize capitalization a bit
            clean = alt.replace("Avatar", "").replace("avatar", "").strip()
            if clean:
                # Title-case but keep acronyms-ish
                team_name = clean
                break

        # -----------------------------
        # Rank:
        # In the text, rank is like "1. 1", "2. 2", etc.
        # We'll try to find the first integer in row text,
        # knowing this might not be perfect but good enough.
        # -----------------------------
        rank = None
        row_text = " ".join(row.stripped_strings)
        m_rank = re.search(r"\b(\d+)\.\s*\1\b", row_text)
        if m_rank:
            rank = int(m_rank.group(1))
        else:
            # fallback: any leading integer
            m2 = re.search(r"^\s*(\d+)\b", row_text)
            if m2:
                rank = int(m2.group(1))

        # -----------------------------
        # NIL valuation:
        # Search inside this row for the first string like $1.9M, $500K, etc.
        # -----------------------------
        nil_str = None
        for txt in row.stripped_strings:
            if txt.startswith("$"):
                # Usually NIL valuation is the first "$" token in the row
                nil_str = txt
                break
        nil_dollars = parse_money_str(nil_str) if nil_str else None

        records.append(
            {
                "rank": rank,
                "position": position,
                "player_name": player_name,
                "player_url": player_href,
                "team_name": team_name,
                "nil_valuation_str": nil_str,
                "nil_valuation_dollars": nil_dollars,
            }
        )

    df = pd.DataFrame(records)
    # Sort by rank if available, else by nil value
    if "rank" in df.columns and df["rank"].notna().any():
        df = df.sort_values("rank")
    elif "nil_valuation_dollars" in df.columns and df["nil_valuation_dollars"].notna().any():
        df = df.sort_values("nil_valuation_dollars", ascending=False)

    print(f"[PARSE] Parsed {len(df)} valid player rows.")
    return df


# -------------------------------------------------------------------
# TEAM-LEVEL SUMMARY
# -------------------------------------------------------------------

def summarize_by_team(players_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate NIL valuations by team.
    Returns a DataFrame with:
      - team_name
      - players_count
      - total_nil_valuation
      - avg_nil_valuation
      - max_nil_valuation
    """
    df = players_df.copy()
    df = df.dropna(subset=["team_name", "nil_valuation_dollars"])

    team_summary = (
        df.groupby("team_name", as_index=False)
        .agg(
            players_count=("player_name", "nunique"),
            total_nil_valuation=("nil_valuation_dollars", "sum"),
            avg_nil_valuation=("nil_valuation_dollars", "mean"),
            max_nil_valuation=("nil_valuation_dollars", "max"),
        )
        .sort_values("total_nil_valuation", ascending=False)
    )

    return team_summary


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main():
    # 1) Fetch NIL rankings HTML (college by default)
    html = fetch_html(RANKINGS_URL)

    # 2) Parse into player-level NIL table
    players_df = parse_on3_nil_rankings_page(html)

    players_out = os.path.join(PROCESSED_DIR, "on3_nil_players_sample.csv")
    players_df.to_csv(players_out, index=False)
    print(f"[OK] Saved player-level NIL sample → {players_out}")

    # 3) Summarize by team
    team_df = summarize_by_team(players_df)

    team_out = os.path.join(PROCESSED_DIR, "on3_nil_teams_sample.csv")
    team_df.to_csv(team_out, index=False)
    print(f"[OK] Saved team-level NIL summary → {team_out}")

    # 4) Print top 15 teams (sanity check)
    print("\n=== Top 15 Teams by Total NIL Valuation (Sample) ===")
    with pd.option_context("display.max_rows", 15, "display.max_columns", None):
        print(team_df.head(15))


if __name__ == "__main__":
    main()
