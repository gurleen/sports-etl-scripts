"""Load FanGraphs park factors (downloaded via the browser) into the park_factors table.

The FanGraphs guts park-factor endpoint is behind Cloudflare, so the CSV is
fetched through a real browser session and saved to disk, then loaded here.
Maps FanGraphs team nicknames (incl. franchise-rename variants) to the
abbreviations used by the `teams` / `park_factors` tables.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import psycopg2

FG_NICKNAME_TO_ABBR = {
    "Angels": "LAA", "Astros": "HOU", "Athletics": "ATH", "Blue Jays": "TOR",
    "Braves": "ATL", "Brewers": "MIL", "Cardinals": "STL", "Cubs": "CHC",
    "Cleveland": "CLE", "Indians": "CLE", "Guardians": "CLE",
    "Devil Rays": "TB", "Rays": "TB", "Diamondbacks": "AZ", "Dodgers": "LAD",
    "Expos": "WSH", "Nationals": "WSH", "Giants": "SF", "Mariners": "SEA",
    "Marlins": "MIA", "Mets": "NYM", "Orioles": "BAL", "Padres": "SD",
    "Phillies": "PHI", "Pirates": "PIT", "Rangers": "TEX", "Red Sox": "BOS",
    "Reds": "CIN", "Rockies": "COL", "Royals": "KC", "Tigers": "DET",
    "Twins": "MIN", "White Sox": "CWS", "Yankees": "NYY",
}

COLUMNS = [
    "game_year", "team", "five_yr", "three_yr", "one_yr", "single", "double",
    "triple", "home_run", "strike_out", "walk", "ground_ball", "fly_ball",
    "line_drive", "IFFB", "FIP",
]


def _db_url() -> str:
    for line in Path(__file__).resolve().parents[1].joinpath(".env").read_text().splitlines():
        if line.startswith("DATABASE_URL="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("DATABASE_URL not found in .env")


def main(csv_path: str) -> None:
    rows = []
    unmapped = set()
    with open(csv_path, newline="") as f:
        for r in csv.DictReader(f):
            abbr = FG_NICKNAME_TO_ABBR.get(r["team"])
            if abbr is None:
                unmapped.add(r["team"])
                continue
            rows.append((
                int(r["season"]), abbr,
                *[float(r[c]) for c in (
                    "five_yr", "three_yr", "one_yr", "single", "double", "triple",
                    "home_run", "strike_out", "walk", "ground_ball", "fly_ball",
                    "line_drive", "IFFB", "FIP")],
            ))
    if unmapped:
        raise SystemExit(f"Unmapped FanGraphs team names: {sorted(unmapped)}")

    cols = ", ".join(f'"{c}"' for c in COLUMNS)
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    with psycopg2.connect(_db_url()) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM park_factors")
        cur.executemany(f"INSERT INTO park_factors ({cols}) VALUES ({placeholders})", rows)
        conn.commit()
        cur.execute("SELECT min(game_year), max(game_year), count(*) FROM park_factors")
        print("park_factors loaded:", cur.fetchone())


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else str(Path.home() / "Downloads" / "fg_park_factors.csv"))
