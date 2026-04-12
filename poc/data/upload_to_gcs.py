#!/usr/bin/env python3
"""
upload_to_gcs.py — Upload locally generated Parquet files to a GCS bucket.

Uses google-cloud-storage for native uploads (resumable, parallel).
Falls back to gsutil if the library is unavailable.

Usage:
    python upload_to_gcs.py [--local /opt1/data] [--bucket my-bucket] [--prefix olap_poc/data] [--workers 4]

Env vars (read from .env or shell):
    GCS_BUCKET, GCS_BUCKET_PREFIX, GCS_SA_JSON_PATH
"""

import argparse
import os
import sys
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Try native GCS client
try:
    from google.cloud import storage as gcs
    from google.oauth2 import service_account
    GCS_NATIVE = True
except ImportError:
    GCS_NATIVE = False
    print("WARNING: google-cloud-storage not installed. Will fall back to gsutil.")


def get_gcs_client(sa_json_path: str | None):
    if not GCS_NATIVE:
        return None
    if sa_json_path and Path(sa_json_path).exists():
        creds = service_account.Credentials.from_service_account_file(sa_json_path)
        return gcs.Client(credentials=creds)
    # Use application default credentials
    return gcs.Client()


def upload_file_native(client, bucket_name: str, local_path: Path, gcs_key: str) -> tuple[str, float]:
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(gcs_key)
    t0     = time.perf_counter()
    blob.upload_from_filename(
        str(local_path),
        content_type="application/octet-stream",
        num_retries=3,
    )
    return gcs_key, time.perf_counter() - t0


def upload_file_gsutil(local_path: Path, gcs_uri: str) -> tuple[str, float]:
    import subprocess
    t0 = time.perf_counter()
    result = subprocess.run(
        ["gsutil", "-q", "cp", str(local_path), gcs_uri],
        capture_output=True, text=True
    )
    elapsed = time.perf_counter() - t0
    if result.returncode != 0:
        raise RuntimeError(f"gsutil failed for {local_path}: {result.stderr}")
    return gcs_uri, elapsed


def main():
    parser = argparse.ArgumentParser(description="Upload POC Parquet files to GCS")
    parser.add_argument("--local",   default=os.environ.get("DATA_DIR", "/opt1/data"))
    parser.add_argument("--bucket",  default=os.environ.get("GCS_BUCKET", ""))
    parser.add_argument("--prefix",  default=os.environ.get("GCS_BUCKET_PREFIX", "olap_poc/data"))
    parser.add_argument("--sa-json", default=os.environ.get("GCS_SA_JSON_PATH", ""))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.bucket:
        print("ERROR: --bucket or GCS_BUCKET env var is required.")
        sys.exit(1)

    local_root = Path(args.local)
    if not local_root.exists():
        print(f"ERROR: Local data directory not found: {local_root}")
        sys.exit(1)

    # Collect all Parquet files
    parquet_files = sorted(local_root.rglob("*.parquet"))
    if not parquet_files:
        print(f"ERROR: No .parquet files found under {local_root}")
        sys.exit(1)

    total_bytes = sum(f.stat().st_size for f in parquet_files)
    print(f"Found {len(parquet_files):,} files  |  {total_bytes / 1e9:.2f} GB")
    print(f"Target: gs://{args.bucket}/{args.prefix}/")

    if args.dry_run:
        for f in parquet_files[:5]:
            rel = f.relative_to(local_root)
            print(f"  DRY-RUN: {f} → gs://{args.bucket}/{args.prefix}/{rel}")
        if len(parquet_files) > 5:
            print(f"  ... and {len(parquet_files) - 5} more")
        return

    client = get_gcs_client(args.sa_json or None)

    t_start   = time.perf_counter()
    uploaded  = 0
    failed    = 0
    total_up  = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for local_f in parquet_files:
            rel     = local_f.relative_to(local_root)
            gcs_key = f"{args.prefix}/{rel}".replace("\\", "/")

            if client:
                fut = executor.submit(upload_file_native, client, args.bucket, local_f, gcs_key)
            else:
                gcs_uri = f"gs://{args.bucket}/{gcs_key}"
                fut = executor.submit(upload_file_gsutil, local_f, gcs_uri)
            futures[fut] = local_f

        for fut in as_completed(futures):
            loc = futures[fut]
            try:
                key, elapsed = fut.result()
                sz  = loc.stat().st_size
                uploaded += 1
                total_up += sz
                rate = sz / elapsed / 1e6 if elapsed > 0 else 0
                print(f"  [{uploaded:>5}/{len(parquet_files)}]  {loc.name:<40}  "
                      f"{sz/1e6:6.1f} MB  {rate:5.1f} MB/s", end="\r", flush=True)
            except Exception as exc:
                failed += 1
                print(f"\n  FAILED: {loc}  —  {exc}", file=sys.stderr)

    elapsed_total = time.perf_counter() - t_start
    print(f"\n")
    print(f"Upload complete: {uploaded}/{len(parquet_files)} files  |  "
          f"{total_up / 1e9:.2f} GB  |  "
          f"{elapsed_total:.1f}s  |  "
          f"{total_up / elapsed_total / 1e6:.1f} MB/s avg")
    if failed:
        print(f"FAILURES: {failed} files failed. Check stderr.", file=sys.stderr)
        sys.exit(1)

    # Write upload manifest
    manifest_path = local_root / "upload_manifest.json"
    manifest_path.write_text(json.dumps({
        "bucket": args.bucket,
        "prefix": args.prefix,
        "files_uploaded": uploaded,
        "total_bytes": total_up,
        "elapsed_s": round(elapsed_total, 2),
    }, indent=2))
    print(f"Upload manifest: {manifest_path}")


if __name__ == "__main__":
    main()
