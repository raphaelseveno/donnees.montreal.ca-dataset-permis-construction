# Montréal « Permis de construction » → Supabase

[![Sync status](https://github.com/raphaelseveno/donnees.montreal.ca-dataset-permis-construction/actions/workflows/sync.yml/badge.svg)](https://github.com/raphaelseveno/donnees.montreal.ca-dataset-permis-construction/actions/workflows/sync.yml)

Daily sync of the CSV from the open-data portal
[donnees.montreal.ca/dataset/permis-construction](https://donnees.montreal.ca/dataset/permis-construction)
into a Supabase (Postgres) database.

## How it works

A GitHub Actions workflow ([`.github/workflows/sync.yml`](.github/workflows/sync.yml))
runs every day at 10:23 UTC and executes [`sync.py`](sync.py), which:

1. Queries Montréal's CKAN API for the dataset's CSV resource metadata.
2. Compares the resource's `last_modified` timestamp (and, if needed, the file's
   SHA-256) with the state stored in the `dataset_sync_state` table.
3. **If nothing changed** → exits without touching the data.
4. **If the file changed** → downloads the CSV, loads it into a staging table with
   `COPY` (column names and types are derived from the file itself), then atomically
   swaps the staging table in as `permis_construction`. Readers never see a
   half-loaded table.

The first run performs the initial database creation automatically: it creates both
`dataset_sync_state` and `permis_construction` (plus an index on `numero_permis`
if that column exists in the CSV).

Both tables are created with **Row Level Security enabled and no policies**, so they
are *not* exposed through Supabase's public REST API by default. To read the data
through the API, add a policy in the Supabase SQL editor:

```sql
create policy "public read" on public.permis_construction
  for select using (true);
```

## Setup (one-time)

1. In the Supabase dashboard, open **Connect** (top of the project page) and copy the
   **Session pooler** URI (port `5432`), e.g.
   `postgresql://postgres.<project-ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres`.
   Use the pooler URI, not the direct `db.<ref>.supabase.co` host — GitHub's runners
   have no IPv6 and cannot reach the direct host.
2. In this GitHub repo: **Settings → Secrets and variables → Actions → New repository
   secret**, name it `SUPABASE_DB_URL`, paste the URI (with your real password).
3. Trigger the first run: **Actions → "Sync Montréal permis-construction dataset to
   Supabase" → Run workflow**. This performs the initial table creation and full load.

After that, the daily schedule takes over. You can re-run manually at any time; check
the **force** box to reload even when the CSV is unchanged.

> Note: GitHub disables scheduled workflows after ~60 days without repo activity;
> an occasional commit or a manual run keeps it alive.

## Configuration (optional env vars)

| Variable          | Default               | Purpose                                        |
| ----------------- | --------------------- | ---------------------------------------------- |
| `SUPABASE_DB_URL` | — (required)          | Postgres connection string                     |
| `CKAN_DATASET_ID` | `permis-construction` | CKAN dataset to watch                          |
| `TARGET_TABLE`    | `permis_construction` | Destination table name                         |
| `CSV_URL_HINT`    | `permis`              | Picks the right CSV if the dataset has several |
| `INDEX_COLUMN`    | `numero_permis`       | Column to index after load (if present)        |
| `FORCE_SYNC`      | `false`               | Reload even if file is unchanged               |

## Running locally

```bash
pip install -r requirements.txt
export SUPABASE_DB_URL='postgresql://...'
python sync.py
```
