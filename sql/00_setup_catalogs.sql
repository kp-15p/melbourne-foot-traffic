-- Free Edition gives you one metastore. dev/prod are catalogs, not workspaces.
CREATE CATALOG IF NOT EXISTS ped_dev
  COMMENT 'Melbourne foot traffic — development. Freely truncatable.';
CREATE CATALOG IF NOT EXISTS ped_prod
  COMMENT 'Melbourne foot traffic — production. Scheduled job writes here.';

-- Run the remainder once per catalog.
USE CATALOG ped_prod;

CREATE SCHEMA IF NOT EXISTS landing COMMENT 'Raw files as landed. No tables.';
CREATE SCHEMA IF NOT EXISTS bronze  COMMENT 'Ingested as-is, typed loosely.';
CREATE SCHEMA IF NOT EXISTS silver  COMMENT 'Cleaned, conformed, deduplicated.';
CREATE SCHEMA IF NOT EXISTS gold    COMMENT 'Business aggregates. Read by BI.';
CREATE SCHEMA IF NOT EXISTS ops     COMMENT 'Run logs, quality metrics.';

-- Managed volume: Databricks owns the storage, Unity Catalog owns the path.
-- External locations are unavailable on Free Edition (no custom workspace storage).
CREATE VOLUME IF NOT EXISTS ped_prod.landing.raw
  COMMENT 'Raw API payloads, one folder per source, partitioned by ingest window.';