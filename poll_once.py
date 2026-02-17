import os
import time
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
import requests
import pandas as pd

EVCBATCH_ENDPOINT = "https://datamall2.mytransport.sg/ltaodataservice/EVCBatch"


def get_ev_batch_download_link(account_key: str, timeout: int = 20) -> str:
    headers = {"AccountKey": account_key, "Accept": "application/json"}
    res = requests.get(EVCBATCH_ENDPOINT, headers=headers, timeout=timeout)
    res.raise_for_status()

    payload = res.json()
    value = payload.get("value", [])
    if not value or "Link" not in value[0]:
        raise RuntimeError(f"No 'Link' found in response. Full response:\n{payload}")

    return value[0]["Link"]


def download_ev_batch_json(download_link: str, timeout: int = 60) -> Dict[str, Any]:
    res = requests.get(download_link, timeout=timeout)
    res.raise_for_status()
    return res.json()


def fetch_ev_batch(account_key: str) -> Dict[str, Any]:
    link = get_ev_batch_download_link(account_key)
    return download_ev_batch_json(link)

# --------------------------------------------------------------
#   Status Decoding Reference : LTADataMall API Documentation
# --------------------------------------------------------------

def decode_location_status(code: Any) -> str:
    s = "" if code is None else str(code).strip()
    return {
        "0": "Occupied (all points occupied)",
        "1": "Available (at least one point available)",
        "100": "Not Available (all points not available)",
        "": "Unknown/Not Provided",
    }.get(s, "Unknown/Other")


def decode_point_status(code: Any) -> str:
    s = "" if code is None else str(code).strip()
    return {
        "0": "Occupied",
        "1": "Available",
        "100": "Not Available",
        "": "Unknown/Not Provided",
    }.get(s, "Unknown/Other")


def decode_evid_status(code: Any) -> str:
    s = "" if code is None else str(code).strip()
    return {
        "0": "Occupied",
        "1": "Available",
        "": "Not Available",
    }.get(s, "Unknown/Other")


# --------------------------------------------------------------
#   Flattening the data for easier analysis
# --------------------------------------------------------------

def flatten_evc_batch(payload: Dict[str, Any], fetched_at_utc: str) -> pd.DataFrame:
    last_updated = payload.get("LastUpdatedTime")
    rows: List[Dict[str, Any]] = []

    locations = payload.get("evLocationsData", []) or []
    for loc in locations:
        loc_address = loc.get("address")
        loc_name = loc.get("name")
        loc_long = loc.get("longtitude")  # API uses this spelling

        loc_lat = loc.get("latitude")
        loc_postal = loc.get("postalCode")
        loc_location_id = loc.get("locationId")
        loc_status = loc.get("status")

        charging_points = loc.get("chargingPoints", []) or []
        for cp in charging_points:
            cp_status = cp.get("status")
            cp_operating_hours = cp.get("operatingHours")
            cp_operator = cp.get("operator")
            cp_position = cp.get("position")
            cp_name = cp.get("name")
            cp_id = cp.get("id")

            plug_types = cp.get("plugTypes", []) or []
            for plug in plug_types:
                plug_type = plug.get("plugType")
                plug_power_rating = plug.get("powerRating")
                plug_charging_speed = plug.get("chargingSpeed")
                plug_current = plug.get("current")
                plug_price = plug.get("price")
                plug_price_type = plug.get("priceType")

                ev_ids = plug.get("evIds", []) or []
                for ev in ev_ids:
                    ev_id = ev.get("id")
                    ev_cp_id = ev.get("evCpId")
                    ev_status = ev.get("status")

                    rows.append({
                        "fetchedAtUTC": fetched_at_utc,
                        "LastUpdatedTime": last_updated,

                        # Location

                        "location_address": loc_address,
                        "location_name": loc_name,
                        "location_longtitude": loc_long,
                        "location_latitude": loc_lat,
                        "location_postalCode": loc_postal,
                        "location_locationId": loc_location_id,
                        "location_status": loc_status,
                        "location_status_desc": decode_location_status(loc_status),

                        # Charging point

                        "cp_status": cp_status,
                        "cp_status_desc": decode_point_status(cp_status),
                        "cp_operatingHours": cp_operating_hours,
                        "cp_operator": cp_operator,
                        "cp_position": cp_position,
                        "cp_name": cp_name,
                        "cp_id": cp_id,

                        # Plug

                        "plug_plugType": plug_type,
                        "plug_powerRating": plug_power_rating,
                        "plug_chargingSpeed": plug_charging_speed,
                        "plug_current": plug_current,
                        "plug_price": plug_price,
                        "plug_priceType": plug_price_type,

                        # Connector

                        "ev_id": ev_id,
                        "evCpId": ev_cp_id,
                        "ev_status": ev_status,
                        "ev_status_desc": decode_evid_status(ev_status),
                    })

    df = pd.DataFrame(rows)

    # Keep postal codes as string (protect leading zeros if any)

    if "location_postalCode" in df.columns:
        df["location_postalCode"] = df["location_postalCode"].astype("string")

    # Coerce coords numeric

    for c in ["location_longtitude", "location_latitude"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


# ----------------
# Append to CSV
# ----------------

def append_to_csv(csv_path: Path, df: pd.DataFrame) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = csv_path.exists()
    # If file exists but is empty, treat as new
    has_content = file_exists and csv_path.stat().st_size > 0

    df.to_csv(
        csv_path,
        mode="a",
        index=False,
        header=not has_content,   # write header only if file is new/empty
        encoding="utf-8-sig"
    )


def main():
    account_key = os.environ.get("LTA_ACCOUNT_KEY")
    if not account_key:
        raise RuntimeError("Missing env var LTA_ACCOUNT_KEY. Add it as a GitHub Secret.")

    payload = fetch_ev_batch(account_key)
    fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    snapshot = {
        "fetchedAtUTC": fetched_at,
        "payload": payload
    }

    df = flatten_evc_batch(payload, fetched_at_utc=fetched_at)

    out_csv = Path("data/evc_history.csv")
    append_to_csv(out_csv, df)

    print(f"[OK] fetchedAtUTC={fetched_at} LastUpdatedTime={payload.get('LastUpdatedTime')}")
    print(f"Appended rows: {len(df)}")
    print(f"Saved to: {out_csv}")


if __name__ == "__main__":
    main()
