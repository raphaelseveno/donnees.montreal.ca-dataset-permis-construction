#!/usr/bin/env python3
"""Sync Montréal's "Permis de construction" dataset into a Supabase database.

Flow:
  1. Ask the CKAN API (donnees.montreal.ca) for the dataset's CSV resource metadata.
  2. Compare `last_modified` (and the file's SHA-256) with the state stored in the
     `dataset_sync_state` table in Supabase.
  3. If nothing changed, exit without touching the data.
  4. If the file changed (or FORCE_SYNC=true, or the target table doesn't exist yet),
     download the CSV, load it into a staging table with COPY, then atomically swap
     the staging table in as the live table.

The target table is created automatically from the CSV header, so the very first
run performs the initial database creation.

Required environment:
  SUPABASE_DB_URL   Postgres connection string (use Supabase's *session pooler* URI,
                    port 5432 — GitHub runners have no IPv6, so avoid the direct host).

Optional environment:
  CKAN_DATASET_ID   defaults to "permis-construction"
  TARGET_TABLE      defaults to "permis_construction"
  CSV_URL_HINT      substring used to pick the right CSV when the dataset has
                    several (defaults to "permis")
  INDEX_COLUMN      column to index after each load, if present (defaults to
                    "numero_permis"; set to "" to skip)
  FORCE_SYNC        "true" to reload even if the file is unchanged
"""

import csv
import hashlib
import io
import os
import re
import sys
import tempfile

import psycopg
import requests
from psycopg import sql

CKAN_BASE = "https://donnees.montreal.ca"
DATASET_ID = os.environ.get("CKAN_DATASET_ID", "permis-construction")
TARGET_TABLE = os.environ.get("TARGET_TABLE", "permis_construction")
CSV_URL_HINT = os.environ.get("CSV_URL_HINT", "permis")
INDEX_COLUMN = os.environ.get("INDEX_COLUMN", "numero_permis")
STATE_TABLE = "dataset_sync_state"
FORCE = os.environ.get("FORCE_SYNC", "").lower() in ("1", "true", "yes")
TYPE_SAMPLE_ROWS = 5000
HTTP_TIMEOUT = 120


def log(msg: str) -> None:
    print(msg, flush=True)


def http_get(url: str, **kwargs) -> requests.Response:
    last_exc = None
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=HTTP_TIMEOUT, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001 - retry any transport error
            last_exc = exc
            log(f"  request failed (attempt {attempt + 1}/3): {exc}")
    raise last_exc


def find_csv_resource() -> dict:
    url = f"{CKAN_BASE}/api/3/action/package_show?id={DATASET_ID}"
    payload = http_get(url).json()
    if not payload.get("success"):
        raise RuntimeError(f"CKAN package_show failed: {payload}")
    resources = payload["result"]["resources"]
    csv_resources = [r for r in resources if (r.get("format") or "").upper() == "CSV"]
    if not csv_resources:
        raise RuntimeError(f"No CSV resource found in dataset {DATASET_ID}")
    if len(csv_resources) > 1 and CSV_URL_HINT:
        preferred = [r for r in csv_resources if CSV_URL_HINT in (r.get("url") or "").lower()]
        if preferred:
            return preferred[0]
    return csv_resources[0]


def download_csv(url: str) -> tuple[str, str]:
    """Stream the CSV to a temp file; return (path, sha256)."""
    digest = hashlib.sha256()
    fd, path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, "wb") as out:
        with requests.get(url, stream=True, timeout=HTTP_TIMEOUT) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=1 << 20):
                out.write(chunk)
                digest.update(chunk)
    return path, digest.hexdigest()


def open_csv(path: str):
    """Open the CSV with a tolerant encoding (Montréal files are UTF-8, some older ones latin-1)."""
    raw = open(path, "rb").read(4096)
    encoding = "utf-8-sig"
    try:
        raw.decode("utf-8")
    except UnicodeDecodeError:
        encoding = "latin-1"
    return open(path, "r", encoding=encoding, newline="")


def sanitize_columns(header: list[str]) -> list[str]:
    cols, seen = [], set()
    for i, name in enumerate(header):
        col = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_") or f"col_{i}"
        base, n = col, 1
        while col in seen:
            n += 1
            col = f"{base}_{n}"
        seen.add(col)
        cols.append(col)
    return cols


_INT_RE = re.compile(r"^-?\d+$")
_FLOAT_RE = re.compile(r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$")


def infer_types(path: str, ncols: int) -> list[str]:
    """Sample rows to pick BIGINT / DOUBLE PRECISION / TEXT per column."""
    is_int = [True] * ncols
    is_num = [True] * ncols
    with open_csv(path) as fh:
        reader = csv.reader(fh)
        next(reader, None)  # header
        for i, row in enumerate(reader):
            if i >= TYPE_SAMPLE_ROWS:
                break
            for j in range(min(len(row), ncols)):
                v = row[j].strip()
                if v == "":
                    continue
                if is_int[j] and not _INT_RE.match(v):
                    is_int[j] = False
                if is_num[j] and not _FLOAT_RE.match(v):
                    is_num[j] = False
    return [
        "BIGINT" if is_int[j] else "DOUBLE PRECISION" if is_num[j] else "TEXT"
        for j in range(ncols)
    ]


def ensure_state_table(conn: psycopg.Connection) -> None:
    conn.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {} (
                dataset_id    text PRIMARY KEY,
                resource_id   text,
                last_modified text,
                file_sha256   text,
                row_count     bigint,
                synced_at     timestamptz NOT NULL DEFAULT now()
            )
            """
        ).format(sql.Identifier(STATE_TABLE))
    )
    conn.execute(
        sql.SQL("ALTER TABLE {} ENABLE ROW LEVEL SECURITY").format(sql.Identifier(STATE_TABLE))
    )
    conn.commit()


def get_state(conn: psycopg.Connection) -> tuple | None:
    row = conn.execute(
        sql.SQL("SELECT last_modified, file_sha256 FROM {} WHERE dataset_id = %s").format(
            sql.Identifier(STATE_TABLE)
        ),
        (DATASET_ID,),
    ).fetchone()
    return row


def save_state(conn, resource_id, last_modified, sha256, row_count) -> None:
    conn.execute(
        sql.SQL(
            """
            INSERT INTO {} (dataset_id, resource_id, last_modified, file_sha256, row_count, synced_at)
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (dataset_id) DO UPDATE SET
                resource_id = EXCLUDED.resource_id,
                last_modified = EXCLUDED.last_modified,
                file_sha256 = EXCLUDED.file_sha256,
                row_count = EXCLUDED.row_count,
                synced_at = now()
            """
        ).format(sql.Identifier(STATE_TABLE)),
        (DATASET_ID, resource_id, last_modified, sha256, row_count),
    )
    conn.commit()


def target_table_exists(conn: psycopg.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s",
        (TARGET_TABLE,),
    ).fetchone()
    return row is not None


def load_into_staging(conn, path, columns, types) -> int:
    staging = f"{TARGET_TABLE}_staging"
    conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(staging)))
    col_defs = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t))
        for c, t in zip(columns, types)
    )
    conn.execute(sql.SQL("CREATE TABLE {} ({})").format(sql.Identifier(staging), col_defs))

    count = 0
    copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(staging),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )
    with open_csv(path) as fh:
        reader = csv.reader(fh)
        next(reader, None)  # header
        with conn.cursor() as cur, cur.copy(copy_stmt) as copy:
            for row in reader:
                if not row:
                    continue
                # Pad/trim to the header width, empty string -> NULL.
                vals = [(row[j].strip() or None) if j < len(row) else None for j in range(len(columns))]
                copy.write_row(vals)
                count += 1
    return count


def swap_in_staging(conn, columns) -> None:
    staging = f"{TARGET_TABLE}_staging"
    old = f"{TARGET_TABLE}_old"
    if INDEX_COLUMN and INDEX_COLUMN in columns:
        conn.execute(
            sql.SQL("CREATE INDEX ON {} ({})").format(
                sql.Identifier(staging), sql.Identifier(INDEX_COLUMN)
            )
        )
    conn.execute(sql.SQL("ALTER TABLE {} ENABLE ROW LEVEL SECURITY").format(sql.Identifier(staging)))
    conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(old)))
    conn.execute(
        sql.SQL("ALTER TABLE IF EXISTS {} RENAME TO {}").format(
            sql.Identifier(TARGET_TABLE), sql.Identifier(old)
        )
    )
    conn.execute(
        sql.SQL("ALTER TABLE {} RENAME TO {}").format(
            sql.Identifier(staging), sql.Identifier(TARGET_TABLE)
        )
    )
    conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(old)))
    conn.commit()
    conn.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(TARGET_TABLE)))
    conn.commit()


def main() -> int:
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        log("ERROR: SUPABASE_DB_URL is not set. Add it as a GitHub Actions secret "
            "(Supabase Dashboard -> Connect -> Session pooler URI).")
        return 1

    log(f"Checking dataset '{DATASET_ID}' on {CKAN_BASE} ...")
    resource = find_csv_resource()
    resource_id = resource.get("id")
    last_modified = resource.get("last_modified") or resource.get("metadata_modified") or ""
    csv_url = resource["url"]
    log(f"  CSV resource: {csv_url}")
    log(f"  last_modified: {last_modified or '(not provided)'}")

    with psycopg.connect(db_url) as conn:
        ensure_state_table(conn)
        state = get_state(conn)
        table_exists = target_table_exists(conn)

        if state and table_exists and not FORCE and last_modified and state[0] == last_modified:
            log("No change detected (last_modified matches). Nothing to do.")
            return 0

        log("Downloading CSV ...")
        path, sha256 = download_csv(csv_url)
        try:
            size_mb = os.path.getsize(path) / (1 << 20)
            log(f"  downloaded {size_mb:.1f} MiB, sha256={sha256[:16]}...")

            if state and table_exists and not FORCE and state[1] == sha256:
                log("File content unchanged (same SHA-256). Updating state only.")
                with open_csv(path) as fh:
                    row_count = sum(1 for _ in fh) - 1
                save_state(conn, resource_id, last_modified, sha256, row_count)
                return 0

            with open_csv(path) as fh:
                header = next(csv.reader(fh))
            columns = sanitize_columns(header)
            log(f"  {len(columns)} columns: {', '.join(columns[:8])}{' ...' if len(columns) > 8 else ''}")

            types = infer_types(path, len(columns))
            log("Loading into staging table ...")
            try:
                row_count = load_into_staging(conn, path, columns, types)
            except psycopg.DataError as exc:
                log(f"  typed load failed ({exc}); retrying with all-TEXT columns ...")
                conn.rollback()
                types = ["TEXT"] * len(columns)
                row_count = load_into_staging(conn, path, columns, types)

            log(f"  loaded {row_count} rows. Swapping into '{TARGET_TABLE}' ...")
            swap_in_staging(conn, columns)
            save_state(conn, resource_id, last_modified, sha256, row_count)
            log(f"Done: '{TARGET_TABLE}' now has {row_count} rows.")
        finally:
            os.unlink(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
