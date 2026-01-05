import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
INDEX_CSV = ROOT / "data_raw" / "espn_core" / "index" / "athletes_index_flat.csv"
INDEX_PARQUET = ROOT / "data_raw" / "espn_core" / "index" / "athletes_index_flat.parquet"
IDS_SEEN = ROOT / "data_raw" / "verify" / "espn_ids_seen.json"
OUT_PATH = ROOT / "data_raw" / "espn_core" / "index" / "espn_name_map.json"


def load_index():
    if INDEX_CSV.exists():
        try:
            return pd.read_csv(INDEX_CSV, dtype={"id": "string"})
        except Exception:
            pass
    if INDEX_PARQUET.exists():
        return pd.read_parquet(INDEX_PARQUET)
    raise FileNotFoundError("Missing ESPN athletes index CSV/parquet.")


def load_ids_seen():
    if not IDS_SEEN.exists():
        return None
    try:
        payload = json.loads(IDS_SEEN.read_text(encoding="utf-8"))
    except Exception:
        return None
    ids = payload.get("ids") if isinstance(payload, dict) else None
    if not isinstance(ids, list):
        return None
    return {str(val).strip() for val in ids if val is not None and str(val).strip()}


def main():
    df = load_index()
    ids_seen = load_ids_seen()
    if ids_seen:
        df = df[df["id"].astype(str).isin(ids_seen)]
    name_map = {}
    for _, row in df.iterrows():
        espn_id = row.get("id")
        if espn_id is None:
            continue
        espn_id = str(espn_id).strip()
        if not espn_id:
            continue
        display = (
            row.get("displayName")
            or row.get("fullName")
            or row.get("shortName")
            or ""
        )
        display = str(display).strip()
        if display and espn_id not in name_map:
            name_map[espn_id] = display
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(name_map, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_PATH} ({len(name_map)} rows)")


if __name__ == "__main__":
    main()
