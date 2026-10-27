"""Atomic, append-only DLQ persistence."""

import json
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def write_dlq(path: Path, rejected: list[dict], source: str, run_id: str, batch_id: str) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    prior = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    known = {(entry.get("batch_id"), entry.get("record_fingerprint")) for entry in prior}
    for item in rejected:
        original = item["record"]
        fingerprint = hashlib.sha256(
            json.dumps(original, sort_keys=True, default=str).encode()
        ).hexdigest()
        if (batch_id, fingerprint) in known:
            continue
        prior.append(
            {
                "record": original,
                "source_type": source,
                "errors": item["errors"],
                "error_message": "; ".join(e.get("msg", "invalid") for e in item["errors"]),
                "run_id": run_id,
                "batch_id": batch_id,
                "record_fingerprint": fingerprint,
                "rejected_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        known.add((batch_id, fingerprint))
    temporary = path.with_name(path.name + f".{uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(prior, indent=2, default=str), encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return len(rejected)
