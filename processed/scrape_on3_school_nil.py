#!/usr/bin/env python3
import re
import json
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.on3.com/nil/rankings/player/nil-valuations/"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}


# -----------------------------------------------------------
# Extract ALL JS chunk URLs from the page
# -----------------------------------------------------------
def get_all_chunks():
    print("[INFO] Fetching NIL valuations page…")
    html = requests.get(BASE_URL, headers=HEADERS).text
    soup = BeautifulSoup(html, "html.parser")

    chunk_urls = []

    for s in soup.find_all("script", src=True):
        src = s["src"]
        if "/_next/static/chunks/" in src:
            url = "https://www.on3.com" + src if src.startswith("/") else src
            chunk_urls.append(url)

    print(f"[INFO] Found {len(chunk_urls)} JS chunks")
    return chunk_urls


# -----------------------------------------------------------
# Robust JSON extractor (nesting-aware)
# -----------------------------------------------------------
def extract_json_objects(js_text):
    objs = []
    depth = 0
    start = None

    for i, char in enumerate(js_text):
        if char == "{":
            if depth == 0:
                start = i
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0 and start is not None:
                objs.append(js_text[start:i+1])
                start = None

    return objs


# -----------------------------------------------------------
# Identify the NIL dataset JSON
# -----------------------------------------------------------
def looks_like_player_json(js):
    # Key NIL indicators
    keys = ["valuation", "athlete", "fullName", "rank", "team", "sport"]

    return any(k in js for k in keys)


# -----------------------------------------------------------
# Recursively extract all player objects
# -----------------------------------------------------------
def extract_players(node):
    players = []

    def walk(x):
        if isinstance(x, dict):
            # Heuristic: dict with valuation fields
            if "valuation" in x and ("fullName" in x or "athlete" in x):
                players.append(x)

            for v in x.values():
                walk(v)

        elif isinstance(x, list):
            for item in x:
                walk(item)

    walk(node)
    return players


# -----------------------------------------------------------
# MAIN SCRAPER
# -----------------------------------------------------------
def main():
    chunk_urls = get_all_chunks()

    all_players = []

    for url in chunk_urls:
        print(f"[INFO] Downloading chunk: {url}")
        js = requests.get(url, headers=HEADERS).text

        json_candidates = extract_json_objects(js)
        print(f"  → {len(json_candidates)} JSON candidates")

        for cand in json_candidates:
            if not looks_like_player_json(cand):
                continue

            try:
                data = json.loads(cand)
            except Exception:
                continue

            players = extract_players(data)
            if players:
                print(f"[SUCCESS] Found {len(players)} NIL players in {url}")
                all_players.extend(players)

    if not all_players:
        print("[ERROR] No NIL players found across all chunks.")
        return

    # Deduplicate by id/fullName
    unique = {p.get("id", p.get("fullName", str(i))): p for i, p in enumerate(all_players)}

    print(f"[INFO] Total unique players extracted: {len(unique)}")

    # Save output
    with open("on3_nil_players.json", "w") as f:
        json.dump(list(unique.values()), f, indent=2)

    print("[DONE] Saved → on3_nil_players.json")


if __name__ == "__main__":
    main()
