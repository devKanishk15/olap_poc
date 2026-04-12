#!/usr/bin/env python3
"""
generate_data.py — Synthetic 10M-row dataset generator for the OLAP POC.

Produces hive-partitioned Parquet files in:
  /opt1/data/event_fact/event_date=YYYY-MM-DD/part-NNNN.parquet

Uses deterministic seed=42 so all three engines ingest identical data.
Streams data in configurable chunk sizes to stay within 8 GB RAM.

Usage:
    python generate_data.py [--rows 10000000] [--seed 42] [--out /opt1/data] [--chunk 250000]
"""

import argparse
import os
import sys
import time
import json
import random
import hashlib
import struct
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TOTAL_ROWS_DEFAULT   = 10_000_000
SEED_DEFAULT         = 42
OUT_DIR_DEFAULT      = "/opt1/data"
CHUNK_SIZE_DEFAULT   = 250_000          # rows per write batch (~100 MB/chunk)
START_DATE           = date(2024, 1, 1)
END_DATE             = date(2024, 1, 30)  # 30 days

EVENT_TYPES          = ["click", "view", "purchase", "scroll", "hover", "search",
                         "add_to_cart", "remove_from_cart", "checkout", "share",
                         "download", "video_play", "form_submit", "login", "logout",
                         "sign_up", "page_load", "error", "impression", "conversion"]

EVENT_SUBTYPES       = [f"subtype_{i:03d}" for i in range(200)]

CAMPAIGN_CHANNELS    = ["email", "paid_search", "organic", "direct", "social",
                         "affiliate", "referral", "display", "push_notification"]

AB_VARIANTS          = ["A", "B", "C", "control"]

COUNTRY_CODES        = ["US", "GB", "DE", "FR", "IN", "JP", "BR", "CA", "AU", "MX",
                         "KR", "IT", "ES", "NL", "SG", "SE", "NO", "DK", "FI", "PL",
                         "ZA", "NG", "EG", "AR", "CL", "CO", "PE", "PH", "ID", "TH",
                         "VN", "PK", "BD", "UA", "RO", "CZ", "HU", "PT", "GR", "BE",
                         "CH", "AT", "TR", "IL", "AE", "SA", "QA", "NZ", "IE", "HK",
                         "TW", "MY", "RU", "CN", "NG", "KE", "GH", "ET", "TZ", "UG"]

OS_FAMILIES          = ["Windows", "macOS", "iOS", "Android", "Linux"]
BROWSER_FAMILIES     = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]
DEVICE_TYPES         = ["desktop", "mobile", "tablet"]

CAT_L1               = [f"category_{i}" for i in range(30)]
CAT_L2               = [f"subcat_{i:03d}" for i in range(300)]

NUM_USERS            = 500_000
NUM_DEVICES          = 600_000
NUM_PAGES            = 5_000
NUM_CAMPAIGNS        = 1_000
NUM_PRODUCTS         = 50_000
NUM_COUPONS          = 500
NUM_EXPERIMENTS      = 20
NUM_SERVERS          = 32


def crc64(data: bytes) -> int:
    """Simple CRC-64 approximation using SHA-256 first 8 bytes."""
    h = hashlib.sha256(data).digest()
    return struct.unpack(">q", h[:8])[0]


def make_arrow_schema() -> pa.Schema:
    return pa.schema([
        pa.field("event_id",               pa.int64(),     nullable=False),
        pa.field("event_date",             pa.date32(),    nullable=False),
        pa.field("event_ts",               pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("session_id",             pa.string(),    nullable=False),
        pa.field("user_id",                pa.int64(),     nullable=False),
        pa.field("device_id",              pa.string(),    nullable=False),
        pa.field("event_type",             pa.string(),    nullable=False),
        pa.field("event_subtype",          pa.string(),    nullable=True),
        pa.field("page_id",                pa.int32(),     nullable=False),
        pa.field("page_name",              pa.string(),    nullable=True),
        pa.field("referrer_url",           pa.string(),    nullable=True),
        pa.field("campaign_id",            pa.string(),    nullable=True),
        pa.field("campaign_channel",       pa.string(),    nullable=True),
        pa.field("ab_variant",             pa.string(),    nullable=True),
        pa.field("country_code",           pa.string(),    nullable=True),
        pa.field("region",                 pa.string(),    nullable=True),
        pa.field("city",                   pa.string(),    nullable=True),
        pa.field("latitude",               pa.float64(),   nullable=True),
        pa.field("longitude",              pa.float64(),   nullable=True),
        pa.field("ip_address",             pa.string(),    nullable=True),
        pa.field("user_agent",             pa.string(),    nullable=True),
        pa.field("os_family",              pa.string(),    nullable=True),
        pa.field("browser_family",         pa.string(),    nullable=True),
        pa.field("device_type",            pa.string(),    nullable=True),
        pa.field("screen_width",           pa.int16(),     nullable=True),
        pa.field("screen_height",          pa.int16(),     nullable=True),
        pa.field("viewport_width",         pa.int16(),     nullable=True),
        pa.field("viewport_height",        pa.int16(),     nullable=True),
        pa.field("product_id",             pa.int32(),     nullable=True),
        pa.field("product_name",           pa.string(),    nullable=True),
        pa.field("product_category_l1",    pa.string(),    nullable=True),
        pa.field("product_category_l2",    pa.string(),    nullable=True),
        pa.field("product_price",          pa.decimal128(12, 2), nullable=True),
        pa.field("quantity",               pa.int16(),     nullable=True),
        pa.field("order_id",               pa.int64(),     nullable=True),
        pa.field("order_total",            pa.decimal128(14, 2), nullable=True),
        pa.field("discount_amount",        pa.decimal128(10, 2), nullable=True),
        pa.field("coupon_code",            pa.string(),    nullable=True),
        pa.field("revenue",                pa.decimal128(14, 4), nullable=True),
        pa.field("duration_ms",            pa.int32(),     nullable=True),
        pa.field("scroll_depth_pct",       pa.int8(),      nullable=True),
        pa.field("click_x",                pa.int16(),     nullable=True),
        pa.field("click_y",                pa.int16(),     nullable=True),
        pa.field("is_bot",                 pa.bool_(),     nullable=False),
        pa.field("is_authenticated",       pa.bool_(),     nullable=False),
        pa.field("is_first_visit",         pa.bool_(),     nullable=False),
        pa.field("experiment_id",          pa.int32(),     nullable=True),
        pa.field("server_id",              pa.int16(),     nullable=True),
        pa.field("load_time_ms",           pa.int32(),     nullable=True),
        pa.field("ttfb_ms",                pa.int16(),     nullable=True),
        pa.field("error_code",             pa.int16(),     nullable=True),
        pa.field("error_message",          pa.string(),    nullable=True),
        pa.field("tag_list",               pa.string(),    nullable=True),
        pa.field("custom_dimensions",      pa.string(),    nullable=True),
        pa.field("raw_payload_size_bytes", pa.int32(),     nullable=True),
        pa.field("ingestion_ts",           pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("processing_lag_ms",      pa.int32(),     nullable=True),
        pa.field("data_version",           pa.int8(),      nullable=False),
        pa.field("partition_key",          pa.int32(),     nullable=False),
        pa.field("checksum",               pa.int64(),     nullable=True),
    ])


def generate_chunk(rng: np.random.Generator, start_id: int, n: int) -> pa.Table:
    """Generate `n` rows starting from event_id=start_id."""

    event_ids = np.arange(start_id, start_id + n, dtype=np.int64)

    # Date distribution: uniform across 30-day window
    date_offsets = rng.integers(0, (END_DATE - START_DATE).days, size=n)
    event_dates  = np.array([START_DATE + timedelta(days=int(d)) for d in date_offsets])
    # Timestamps: event_date + random hour/min/sec/us
    ts_seconds   = rng.integers(0, 86_400, size=n)
    ts_us        = rng.integers(0, 1_000_000, size=n)
    event_ts     = np.array([
        datetime(ed.year, ed.month, ed.day, tzinfo=timezone.utc).timestamp() * 1_000_000
        + ts_seconds[i] * 1_000_000 + ts_us[i]
        for i, ed in enumerate(event_dates)
    ], dtype="datetime64[us]")

    user_ids     = rng.integers(1, NUM_USERS + 1, size=n).astype(np.int64)
    page_ids     = rng.integers(1, NUM_PAGES + 1, size=n).astype(np.int32)
    server_ids   = rng.integers(1, NUM_SERVERS + 1, size=n).astype(np.int16)

    # Low-cardinality fields
    ev_type      = rng.choice(EVENT_TYPES, size=n)
    ev_subtype   = np.where(rng.random(n) < 0.9, rng.choice(EVENT_SUBTYPES, size=n), None)
    chan         = np.where(rng.random(n) < 0.8, rng.choice(CAMPAIGN_CHANNELS, size=n), None)
    ab_var       = np.where(rng.random(n) < 0.7, rng.choice(AB_VARIANTS, size=n), None)
    country      = rng.choice(COUNTRY_CODES, size=n)
    os_fam       = rng.choice(OS_FAMILIES, size=n)
    br_fam       = rng.choice(BROWSER_FAMILIES, size=n)
    dev_type     = rng.choice(DEVICE_TYPES, size=n)

    # Commerce fields — only populated for purchase-like events
    is_purchase  = np.isin(ev_type, ["purchase", "checkout", "conversion"])
    is_cart      = np.isin(ev_type, ["add_to_cart", "remove_from_cart"])
    has_product  = is_purchase | is_cart | (rng.random(n) < 0.3)

    product_ids  = np.where(has_product, rng.integers(1, NUM_PRODUCTS + 1, size=n).astype(np.int32), None)
    prices_raw   = np.round(rng.uniform(0.99, 999.99, size=n), 2)
    product_price= np.where(has_product, prices_raw, None)
    qty_raw      = rng.integers(1, 21, size=n).astype(np.int16)
    quantity     = np.where(has_product, qty_raw, None)

    order_ids       = np.where(is_purchase, rng.integers(1, 5_000_001, size=n).astype(np.int64), None)
    order_totals_raw= np.round(prices_raw * qty_raw, 2)
    order_totals    = np.where(is_purchase, order_totals_raw, None)
    discount_pct    = rng.uniform(0, 0.3, size=n)
    disc_amts_raw   = np.round(order_totals_raw * discount_pct, 2)
    disc_amts       = np.where(is_purchase, disc_amts_raw, None)
    coupons      = np.where(
        is_purchase & (rng.random(n) < 0.3),
        [f"COUP{rng.integers(1, NUM_COUPONS + 1):04d}" for _ in range(n)],
        None
    )
    revenue_arr  = np.where(is_purchase, np.round((order_totals_raw - disc_amts_raw) * 0.1, 4), None)

    # Booleans
    is_bot       = rng.random(n) < 0.02
    is_auth      = rng.random(n) < 0.55
    is_first     = rng.random(n) < 0.15

    # Error codes — sparse
    has_error    = rng.random(n) < 0.05
    err_codes_raw= rng.choice([400, 401, 403, 404, 429, 500, 502, 503], size=n).astype(np.int16)
    error_codes  = np.where(has_error, err_codes_raw, None)
    error_msgs   = np.where(has_error, [f"HTTP {c}" for c in err_codes_raw], None)

    # Geo
    lats         = np.where(rng.random(n) < 0.8, rng.uniform(-90, 90, size=n), None)
    lons         = np.where(rng.random(n) < 0.8, rng.uniform(-180, 180, size=n), None)

    # Experiments
    exp_ids      = np.where(rng.random(n) < 0.4,
                            rng.integers(1, NUM_EXPERIMENTS + 1, size=n).astype(np.int32), None)

    # Tags
    tag_count    = rng.integers(0, 6, size=n)
    all_tags     = [f"tag_{i}" for i in range(50)]
    tag_lists    = [
        ",".join(rng.choice(all_tags, size=int(tc), replace=False).tolist())
        if tc > 0 else None
        for tc in tag_count
    ]

    # Custom dimensions JSON
    cd_keys      = ["theme", "locale", "plan", "cohort", "flag"]
    cd_vals      = [["dark", "light"], ["en", "de", "fr", "ja"], ["free", "pro", "enterprise"],
                    ["A", "B", "C"], ["true", "false"]]
    custom_dims  = [
        json.dumps({k: rng.choice(v).item() for k, v in zip(cd_keys, cd_vals)})
        for _ in range(n)
    ]

    # Ingestion lag: event_ts + random 0–5000 ms
    lag_ms       = rng.integers(50, 5001, size=n).astype(np.int64)
    ingestion_ts = (event_ts.astype(np.int64) + lag_ms * 1000).astype("datetime64[us]")

    # Checksums
    checksums    = np.array([
        crc64(struct.pack(">qq", int(eid), int(uid)))
        for eid, uid in zip(event_ids, user_ids)
    ], dtype=np.int64)

    # Partition key (0–29)
    part_keys    = (event_ids % 30).astype(np.int32)
    data_ver     = rng.integers(1, 6, size=n).astype(np.int8)

    schema = make_arrow_schema()

    arrays = [
        pa.array(event_ids),
        pa.array(event_dates, type=pa.date32()),
        pa.array(event_ts, type=pa.timestamp("us", tz="UTC")),
        pa.array([f"{uid:08x}-{pid:04x}-{sid:04x}-{eid % 65536:04x}-{eid:012x}"
                  for uid, pid, sid, eid in zip(user_ids, page_ids, server_ids, event_ids)]),
        pa.array(user_ids),
        pa.array([f"dev-{did:010x}" for did in rng.integers(1, NUM_DEVICES + 1, size=n)]),
        pa.array(ev_type.tolist()),
        pa.array(ev_subtype.tolist()),
        pa.array(page_ids),
        pa.array([f"page_{pid}" for pid in page_ids]),
        pa.array(np.where(rng.random(n) < 0.85,
                          [f"https://ref{i}.example.com/path" for i in rng.integers(1, 1001, size=n)],
                          None).tolist()),
        pa.array(np.where(rng.random(n) < 0.8,
                          [f"camp_{i:04d}" for i in rng.integers(1, NUM_CAMPAIGNS + 1, size=n)],
                          None).tolist()),
        pa.array(chan.tolist()),
        pa.array(ab_var.tolist()),
        pa.array(country.tolist()),
        pa.array([f"region_{cc}_{rng.integers(1, 20)}" for cc in country]),
        pa.array([f"city_{rng.integers(1, 800)}" for _ in range(n)]),
        pa.array(lats.tolist()),
        pa.array(lons.tolist()),
        pa.array([f"{rng.integers(1,256)}.{rng.integers(0,256)}.{rng.integers(0,256)}.{rng.integers(0,256)}"
                  for _ in range(n)]),
        pa.array([f"Mozilla/5.0 ({os_fam[i]}) {br_fam[i]}/{rng.integers(80, 120)}"
                  for i in range(n)]),
        pa.array(os_fam.tolist()),
        pa.array(br_fam.tolist()),
        pa.array(dev_type.tolist()),
        pa.array(np.where(dev_type == "mobile",
                           rng.choice([375, 390, 414, 430], size=n),
                           rng.choice([1280, 1440, 1920, 2560], size=n)).astype(np.int16).tolist()),
        pa.array(np.where(dev_type == "mobile",
                           rng.choice([667, 812, 896, 932], size=n),
                           rng.choice([720, 900, 1080, 1440], size=n)).astype(np.int16).tolist()),
        pa.array(rng.integers(320, 1921, size=n).astype(np.int16).tolist()),
        pa.array(rng.integers(480, 1081, size=n).astype(np.int16).tolist()),
        pa.array(product_ids.tolist()),
        pa.array([f"product_{pid}" if pid is not None else None for pid in product_ids.tolist()]),
        pa.array([rng.choice(CAT_L1).item() if pid is not None else None for pid in product_ids.tolist()]),
        pa.array([rng.choice(CAT_L2).item() if pid is not None else None for pid in product_ids.tolist()]),
        pa.array(product_price.tolist(), type=pa.float64()).cast(pa.decimal128(12, 2)),
        pa.array(quantity.tolist()),
        pa.array(order_ids.tolist()),
        pa.array(order_totals.tolist(), type=pa.float64()).cast(pa.decimal128(14, 2)),
        pa.array(disc_amts.tolist(), type=pa.float64()).cast(pa.decimal128(10, 2)),
        pa.array(coupons.tolist()),
        pa.array(revenue_arr.tolist(), type=pa.float64()).cast(pa.decimal128(14, 4)),
        pa.array(rng.integers(100, 300_001, size=n).astype(np.int32).tolist()),
        pa.array(rng.integers(0, 101, size=n).astype(np.int8).tolist()),
        pa.array(rng.integers(0, 1921, size=n).astype(np.int16).tolist()),
        pa.array(rng.integers(0, 1081, size=n).astype(np.int16).tolist()),
        pa.array(is_bot.tolist()),
        pa.array(is_auth.tolist()),
        pa.array(is_first.tolist()),
        pa.array(exp_ids.tolist()),
        pa.array(server_ids.tolist()),
        pa.array(rng.integers(50, 10_001, size=n).astype(np.int32).tolist()),
        pa.array(rng.integers(10, 2_001, size=n).astype(np.int16).tolist()),
        pa.array(error_codes.tolist()),
        pa.array(error_msgs.tolist()),
        pa.array(tag_lists),
        pa.array(custom_dims),
        pa.array(rng.integers(100, 50_001, size=n).astype(np.int32).tolist()),
        pa.array(ingestion_ts, type=pa.timestamp("ms", tz="UTC")),
        pa.array(lag_ms.tolist()),
        pa.array(data_ver.tolist()),
        pa.array(part_keys.tolist()),
        pa.array(checksums.tolist()),
    ]

    return pa.table(dict(zip(schema.names, arrays)), schema=schema)


def main():
    parser = argparse.ArgumentParser(description="Generate OLAP POC synthetic dataset")
    parser.add_argument("--rows",  type=int, default=TOTAL_ROWS_DEFAULT)
    parser.add_argument("--seed",  type=int, default=SEED_DEFAULT)
    parser.add_argument("--out",   type=str, default=OUT_DIR_DEFAULT)
    parser.add_argument("--chunk", type=int, default=CHUNK_SIZE_DEFAULT)
    args = parser.parse_args()

    out_root = Path(args.out) / "event_fact"
    rng = np.random.default_rng(args.seed)

    total    = args.rows
    chunk_sz = args.chunk
    n_chunks = (total + chunk_sz - 1) // chunk_sz

    print(f"Generating {total:,} rows  |  chunk={chunk_sz:,}  |  seed={args.seed}")
    print(f"Output root: {out_root}")

    t0 = time.perf_counter()
    rows_written = 0
    chunk_idx    = 0

    while rows_written < total:
        this_chunk = min(chunk_sz, total - rows_written)
        table      = generate_chunk(rng, start_id=rows_written + 1, n=this_chunk)

        # Group by event_date for hive partitioning
        dates_in_chunk = table.column("event_date").to_pylist()
        unique_dates   = sorted(set(dates_in_chunk))

        for ed in unique_dates:
            mask      = pa.compute.equal(table.column("event_date"), ed)
            sub_table = table.filter(mask)
            part_dir  = out_root / f"event_date={ed}"
            part_dir.mkdir(parents=True, exist_ok=True)
            part_file = part_dir / f"part-{chunk_idx:05d}.parquet"
            pq.write_table(
                sub_table,
                str(part_file),
                compression="snappy",
                row_group_size=100_000,
            )

        rows_written += this_chunk
        chunk_idx    += 1
        elapsed       = time.perf_counter() - t0
        rate          = rows_written / elapsed
        pct           = rows_written / total * 100
        print(f"  [{pct:5.1f}%]  {rows_written:>10,} rows  |  "
              f"chunk {chunk_idx}/{n_chunks}  |  {rate:,.0f} rows/s  |  {elapsed:.1f}s elapsed",
              end="\r", flush=True)

    elapsed = time.perf_counter() - t0
    print(f"\nDone: {rows_written:,} rows in {elapsed:.1f}s  ({rows_written/elapsed:,.0f} rows/s)")
    print(f"Output: {out_root}")

    # Write manifest
    manifest_path = Path(args.out) / "manifest.json"
    manifest = {
        "rows": rows_written,
        "seed": args.seed,
        "start_date": str(START_DATE),
        "end_date": str(END_DATE),
        "chunk_size": args.chunk,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "out_dir": str(out_root),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
