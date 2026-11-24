import hashlib
import json
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any
import re

import pandas as pd


COUNTRY_TABLES = {
    "IND": "TABLE_IND",
    "USA": "TABLE_USA",
    "AUS": "TABLE_AUS",
    "AU": "TABLE_AUS",
    "PHIL": "TABLE_PHIL",
    "CAN": "TABLE_CAN",
}
DEFAULT_COUNTRY_TABLE = "TABLE_OTHER"


def parse_flexible_date(val: Any) -> date | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    if s.isdigit() and len(s) == 7:
        s = s.zfill(8)
    for fmt in ("%Y%m%d", "%m/%d/%Y", "%m%d%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def compute_age(dob: date | None, as_of: date) -> int | None:
    if dob is None:
        return None
    years = as_of.year - dob.year
    if (as_of.month, as_of.day) < (dob.month, dob.day):
        years -= 1
    return years


def is_stale_member(last_flight_date: date | None, reference_date: date, threshold_days: int = 90) -> bool:
    if last_flight_date is None:
        return False
    return (reference_date - last_flight_date).days > threshold_days


def compute_record_hash(record: dict[str, Any]) -> str:
    canonical = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_pipe_line(line: str) -> tuple[dict[str, str] | None, list[str]]:
    stripped = line.strip()
    if not stripped or stripped == "|":
        return None, ["EMPTY_OR_STRAY_PIPE_LINE"]

    if stripped.startswith("|H|") or stripped.startswith("H|"):
        return None, []

    errors: list[str] = []
    if not stripped.startswith("|D|"):
        errors.append("MISSING_LEADING_PIPE")

    body = stripped.lstrip("|")
    parts = [p.strip() for p in body.split("|")]
    if parts and parts[-1] == "":
        parts.pop()

    if len(parts) != 11 or parts[0] != "D":
        errors.append(f"MALFORMED_COLUMN_COUNT:{len(parts)}")
        return None, errors

    parsed = {
        "member_name": parts[1],
        "member_id": parts[2],
        "enrollment_date": parts[3],
        "last_flight_date": parts[4],
        "tier_code": parts[5],
        "agent_name": parts[6],
        "state_code": parts[7],
        "country_code": parts[8].upper(),
        "dob": parts[9],
        "is_active_flag": parts[10].upper(),
        "member_segment": "",
    }
    return parsed, errors


class SkyPointsPipeline:
    def __init__(self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS STG_MEMBER_PROFILE (
            member_id TEXT NOT NULL,
            member_name TEXT NOT NULL,
            enrollment_date TEXT NOT NULL,
            last_flight_date TEXT,
            tier_code TEXT,
            agent_name TEXT,
            member_segment TEXT,
            state_code TEXT,
            country_code TEXT NOT NULL,
            date_of_birth TEXT,
            is_active_flag TEXT,
            age_years INTEGER,
            stale_member INTEGER NOT NULL,
            source_file_name TEXT NOT NULL,
            file_batch_date TEXT NOT NULL,
            record_seq_num INTEGER NOT NULL,
            record_hash TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS FCT_MEMBER_REDEMPTIONS (
            redemption_txn_id TEXT PRIMARY KEY,
            member_id TEXT NOT NULL,
            feed_date TEXT NOT NULL,
            txn_date TEXT NOT NULL,
            partner_name TEXT NOT NULL,
            miles_redeemed REAL NOT NULL,
            redemption_status TEXT NOT NULL,
            source_file_name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS DQ_REJECTED_RECORDS (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_layer TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            raw_record_key TEXT,
            failure_reasons TEXT NOT NULL,
            raw_payload TEXT
        );
        """)
        for table in set(COUNTRY_TABLES.values()) | {DEFAULT_COUNTRY_TABLE}:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                member_id TEXT PRIMARY KEY,
                member_name TEXT NOT NULL,
                enrollment_date TEXT NOT NULL,
                last_flight_date TEXT,
                tier_code TEXT,
                agent_name TEXT,
                member_segment TEXT,
                state_code TEXT,
                country_code TEXT NOT NULL,
                date_of_birth TEXT,
                is_active_flag TEXT,
                age_years INTEGER,
                stale_member INTEGER NOT NULL,
                effective_batch_date TEXT NOT NULL,
                record_hash TEXT NOT NULL
            )
            """)
        self.conn.commit()

    def _quarantine(self, layer: str, file_name: str, key: str | None, reasons: list[str], payload: str) -> None:
        self.conn.execute(
            """
            INSERT INTO DQ_REJECTED_RECORDS (source_layer, source_file_name, raw_record_key, failure_reasons, raw_payload)
            VALUES (?, ?, ?, ?, ?)
            """,
            (layer, file_name, key, json.dumps(reasons), payload[:1000]),
        )

    def _validate_and_stage(
        self,
        raw_row: dict[str, Any],
        source_file: str,
        batch_date: date,
        seq_num: int,
        as_of_date: date,
    ) -> bool:
        reasons: list[str] = []
        member_id = str(raw_row.get("member_id", "")).strip()
        member_name = str(raw_row.get("member_name", "")).strip()
        country_code = str(raw_row.get("country_code", "")).strip().upper()
        segment = str(raw_row.get("member_segment", "")).strip().upper()

        if not member_id:
            reasons.append("MISSING_MEMBER_ID")
        elif len(member_id) > 18:
            reasons.append("MEMBER_ID_EXCEEDS_18_CHARS")

        if not member_name:
            reasons.append("MISSING_MEMBER_NAME")
        elif len(member_name) > 255:
            reasons.append("MEMBER_NAME_EXCEEDS_255_CHARS")

        if not country_code:
            reasons.append("MISSING_COUNTRY_CODE")

        enroll_dt = parse_flexible_date(raw_row.get("enrollment_date"))
        if enroll_dt is None:
            reasons.append("INVALID_ENROLLMENT_DATE")

        flight_raw = raw_row.get("last_flight_date")
        flight_dt = parse_flexible_date(flight_raw)
        if flight_raw and str(flight_raw).strip() and flight_dt is None:
            reasons.append("INVALID_FLIGHT_DATE")

        dob_raw = raw_row.get("dob")
        dob_dt = parse_flexible_date(dob_raw)
        if dob_raw and str(dob_raw).strip() and dob_dt is None:
            reasons.append("INVALID_DOB")

        if enroll_dt and flight_dt and flight_dt < enroll_dt:
            reasons.append("FLIGHT_DATE_BEFORE_ENROLLMENT")

        if segment and segment not in {"I", "C"}:
            reasons.append(f"INVALID_SEGMENT:{segment}")

        if reasons:
            self._quarantine("MEMBER_PROFILE", source_file, member_id or None, reasons, json.dumps(raw_row, default=str))
            return False

        age = compute_age(dob_dt, as_of_date)
        stale = int(is_stale_member(flight_dt, batch_date))
        clean_record = {
            "member_id": member_id,
            "member_name": member_name,
            "enrollment_date": enroll_dt.isoformat(),
            "last_flight_date": flight_dt.isoformat() if flight_dt else None,
            "tier_code": str(raw_row.get("tier_code", "")).strip().upper() or None,
            "agent_name": str(raw_row.get("agent_name", "")).strip() or None,
            "member_segment": segment or None,
            "state_code": str(raw_row.get("state_code", "")).strip().upper() or None,
            "country_code": country_code,
            "date_of_birth": dob_dt.isoformat() if dob_dt else None,
            "is_active_flag": str(raw_row.get("is_active_flag", "")).strip().upper() or None,
        }
        rec_hash = compute_record_hash(clean_record)

        self.conn.execute(
            """
            INSERT INTO STG_MEMBER_PROFILE (
                member_id, member_name, enrollment_date, last_flight_date,
                tier_code, agent_name, member_segment, state_code, country_code,
                date_of_birth, is_active_flag, age_years, stale_member,
                source_file_name, file_batch_date, record_seq_num, record_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                clean_record["member_id"],
                clean_record["member_name"],
                clean_record["enrollment_date"],
                clean_record["last_flight_date"],
                clean_record["tier_code"],
                clean_record["agent_name"],
                clean_record["member_segment"],
                clean_record["state_code"],
                clean_record["country_code"],
                clean_record["date_of_birth"],
                clean_record["is_active_flag"],
                age,
                stale,
                source_file,
                batch_date.isoformat(),
                seq_num,
                rec_hash,
            ),
        )
        return True

    def ingest_tabular_file(self, file_path: str, batch_date: date, seq_offset: int = 0, as_of_date: date | None = None) -> None:
        as_of = as_of_date or batch_date
        p = Path(file_path)
        country_hint = re.sub(r"[^A-Za-z]", "", p.stem).upper()
        if p.suffix.lower() == ".csv":
            df = pd.read_csv(p, dtype=str).fillna("")
        elif p.suffix.lower() in {".xlsx", ".xls"}:
            df = pd.read_excel(p, dtype=str).fillna("")
        else:
            raise ValueError(f"Unsupported tabular format: {p.suffix}")

        df.columns = [c.strip() for c in df.columns]
        for idx, row in df.iterrows():
            mapped = {
                "member_id": row.get("ID", ""),
                "member_name": row.get("Name", ""),
                "dob": row.get("DOB", ""),
                "tier_code": row.get("TierCode", ""),
                "enrollment_date": row.get("EnrollmentDate", ""),
                "member_segment": row.get("Individual or Corporate", ""),
                "last_flight_date": row.get("Flight Date", ""),
                "country_code": row.get("Country", "") or country_hint,
                "state_code": row.get("State", ""),
                "agent_name": row.get("Agent_Name", ""),
                "is_active_flag": row.get("Is_Active", "A"),
            }
            self._validate_and_stage(mapped, p.name, batch_date, seq_offset + int(idx) + 1, as_of)
        self.conn.commit()

    def ingest_pipe_file(self, file_path: str, batch_date: date, seq_offset: int = 0, as_of_date: date | None = None) -> None:
        as_of = as_of_date or batch_date
        p = Path(file_path)
        with open(p, "r", encoding="utf-8") as fh:
            for idx, line in enumerate(fh, start=1):
                parsed, errors = parse_pipe_line(line)
                if errors:
                    self._quarantine("PIPE_FLAT_FILE", p.name, None, errors, line.strip())
                    continue
                if parsed is None:
                    continue
                self._validate_and_stage(parsed, p.name, batch_date, seq_offset + idx, as_of)
        self.conn.commit()

    def route_to_country_tables(self, batch_date: date) -> None:
        collisions = self.conn.execute(
            """
            SELECT member_id, GROUP_CONCAT(DISTINCT source_file_name) AS files, COUNT(*) AS cnt
            FROM STG_MEMBER_PROFILE
            WHERE file_batch_date = ?
            GROUP BY member_id
            HAVING COUNT(*) > 1
            """,
            (batch_date.isoformat(),),
        ).fetchall()
        for col in collisions:
            self._quarantine(
                "STAGING_DEDUP",
                col["files"],
                col["member_id"],
                [f"DUPLICATE_MEMBER_ID_IN_BATCH:{col['cnt']}"],
                f"Appeared across files: {col['files']}",
            )

        winners = self.conn.execute(
            """
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY member_id
                           ORDER BY file_batch_date DESC, record_seq_num DESC
                       ) AS rn
                FROM STG_MEMBER_PROFILE
                WHERE file_batch_date = ?
            )
            SELECT * FROM ranked WHERE rn = 1
            """,
            (batch_date.isoformat(),),
        ).fetchall()

        all_tables = set(COUNTRY_TABLES.values()) | {DEFAULT_COUNTRY_TABLE}
        for row in winners:
            target_table = COUNTRY_TABLES.get(row["country_code"], DEFAULT_COUNTRY_TABLE)
            for tbl in all_tables:
                if tbl != target_table:
                    self.conn.execute(f"DELETE FROM {tbl} WHERE member_id = ?", (row["member_id"],))

            self.conn.execute(
                f"""
                INSERT INTO {target_table} (
                    member_id, member_name, enrollment_date, last_flight_date,
                    tier_code, agent_name, member_segment, state_code, country_code,
                    date_of_birth, is_active_flag, age_years, stale_member,
                    effective_batch_date, record_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(member_id) DO UPDATE SET
                    member_name = excluded.member_name,
                    enrollment_date = excluded.enrollment_date,
                    last_flight_date = excluded.last_flight_date,
                    tier_code = excluded.tier_code,
                    agent_name = excluded.agent_name,
                    member_segment = excluded.member_segment,
                    state_code = excluded.state_code,
                    country_code = excluded.country_code,
                    date_of_birth = excluded.date_of_birth,
                    is_active_flag = excluded.is_active_flag,
                    age_years = excluded.age_years,
                    stale_member = excluded.stale_member,
                    effective_batch_date = excluded.effective_batch_date,
                    record_hash = excluded.record_hash
                WHERE {target_table}.record_hash <> excluded.record_hash
                """,
                (
                    row["member_id"],
                    row["member_name"],
                    row["enrollment_date"],
                    row["last_flight_date"],
                    row["tier_code"],
                    row["agent_name"],
                    row["member_segment"],
                    row["state_code"],
                    row["country_code"],
                    row["date_of_birth"],
                    row["is_active_flag"],
                    row["age_years"],
                    row["stale_member"],
                    row["file_batch_date"],
                    row["record_hash"],
                ),
            )
        self.conn.commit()

    def ingest_redemption_json(self, file_path: str) -> None:
        p = Path(file_path)
        with open(p, "r", encoding="utf-8") as fh:
            payload = json.load(fh)

        member_id = str(payload.get("member id", "")).strip()
        feed_dt = parse_flexible_date(payload.get("feed_date"))
        redemptions = payload.get("redemptions", [])

        if not member_id or feed_dt is None or not isinstance(redemptions, list):
            self._quarantine("REDEMPTION_JSON", p.name, member_id or None, ["INVALID_REDEMPTION_ENVELOPE"], json.dumps(payload))
            self.conn.commit()
            return

        for txn in redemptions:
            txn_id = str(txn.get("txn_id", "")).strip()
            txn_dt = parse_flexible_date(txn.get("txn_date"))
            partner = str(txn.get("partner", "")).strip()
            miles = txn.get("miles_redeemed")
            status = str(txn.get("status", "")).strip().upper()

            reasons: list[str] = []
            if not txn_id:
                reasons.append("MISSING_TXN_ID")
            if txn_dt is None:
                reasons.append("INVALID_TXN_DATE")
            if miles is None or float(miles) <= 0:
                reasons.append("INVALID_MILES_REDEEMED")
            if status not in {"COMPLETED", "PENDING", "FAILED", "REVERSED"}:
                reasons.append(f"UNKNOWN_STATUS:{status}")

            if reasons:
                self._quarantine("REDEMPTION_JSON", p.name, txn_id or member_id, reasons, json.dumps(txn))
                continue

            self.conn.execute(
                """
                INSERT INTO FCT_MEMBER_REDEMPTIONS (
                    redemption_txn_id, member_id, feed_date, txn_date,
                    partner_name, miles_redeemed, redemption_status, source_file_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(redemption_txn_id) DO UPDATE SET
                    member_id = excluded.member_id,
                    feed_date = excluded.feed_date,
                    txn_date = excluded.txn_date,
                    partner_name = excluded.partner_name,
                    miles_redeemed = excluded.miles_redeemed,
                    redemption_status = excluded.redemption_status,
                    source_file_name = excluded.source_file_name
                """,
                (txn_id, member_id, feed_dt.isoformat(), txn_dt.isoformat(), partner, float(miles), status, p.name),
            )
        self.conn.commit()

    def get_enriched_redemptions(self) -> list[dict[str, Any]]:
        union_sql = " UNION ALL ".join(
            f"SELECT * FROM {tbl}" for tbl in sorted(set(COUNTRY_TABLES.values()) | {DEFAULT_COUNTRY_TABLE})
        )
        rows = self.conn.execute(
            f"""
            SELECT
                r.redemption_txn_id,
                r.member_id,
                r.feed_date,
                r.txn_date,
                r.partner_name,
                r.miles_redeemed,
                r.redemption_status,
                m.member_name,
                m.tier_code,
                m.country_code,
                m.stale_member
            FROM FCT_MEMBER_REDEMPTIONS r
            LEFT JOIN ({union_sql}) m
              ON r.member_id = m.member_id
            ORDER BY r.redemption_txn_id
            """
        ).fetchall()
        return [dict(row) for row in rows]
