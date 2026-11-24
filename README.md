# SkyPoints Data Engineering Technical Assessment

## Architecture & Design Decisions
1. **Multi-Format Ingestion (`CSV`, `XLSX`, Pipe Flat File, JSON):**
   - Normalizes tabular country files (`IND.csv`, `USA.csv`, `AUS.xlsx`), pipe-delimited member feeds, and partner redemption JSON feeds into a unified staging model.
2. **Scale & Snowflake Optimization (Billions of Records/Day):**
   - Uses `TRANSIENT` raw landing tables, clustering keys (`file_batch_date`, `country_code`, `member_id`), deterministic `SHA2` record hashes, and set-based `MERGE` + `QUALIFY ROW_NUMBER()`.
3. **Latest Record Wins & Country Move Handling:**
   - Deduplicates per `member_id` by `file_batch_date DESC, record_seq_num DESC`, upserts into the winning country table (`TABLE_IND`, `TABLE_USA`, `TABLE_AUS`, etc.), and evicts moved members from prior country tables.
4. **Data Quality Quarantine (`DQ_REJECTED_RECORDS`):**
   - Captures malformed pipe rows, missing mandatory fields, zero-stripped dates (`3051985` -> `03051985`), invalid segments, and cross-file ID collisions.

## Quick Start
```bash
pip install pandas openpyxl pytest
pytest -v
```
