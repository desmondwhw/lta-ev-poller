import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import requests

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


def append_jsonl(file_path: Path, record: Dict[str, Any]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False))
        f.write("\n")


def main():
    # GitHub Actions will inject this env var
    import os
    account_key = os.environ.get("LTA_ACCOUNT_KEY")
    if not account_key:
        raise RuntimeError("Missing env var LTA_ACCOUNT_KEY. Add it as a GitHub Secret.")

    payload = fetch_ev_batch(account_key)
    fetched_at = datetime.now(timezone.utc).isoformat()

    snapshot = {
        "fetchedAtUTC": fetched_at,
        "payload": payload
    }

    out_file = Path("data/evc_batch_history.jsonl")
    append_jsonl(out_file, snapshot)

    last_updated = payload.get("LastUpdatedTime")
    loc_count = len(payload.get("evLocationsData", []) or [])
    print(f"[OK] fetchedAtUTC={fetched_at} LastUpdatedTime={last_updated} locations={loc_count}")
    print(f"Appended to {out_file}")


if __name__ == "__main__":
    main()
