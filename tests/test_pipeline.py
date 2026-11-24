from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline import SkyPointsPipeline, compute_age, is_stale_member, parse_flexible_date


DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


def test_date_parsing_and_zero_stripped_dob():
    assert parse_flexible_date("03051985") == date(1985, 3, 5)
    assert parse_flexible_date("3051985") == date(1985, 3, 5)
    assert parse_flexible_date("8/13/1982") == date(1982, 8, 13)
    assert parse_flexible_date("20220615") == date(2022, 6, 15)


def test_age_and_stale_member_derivations():
    assert compute_age(date(1985, 3, 5), date(2024, 3, 4)) == 38
    assert compute_age(date(1985, 3, 5), date(2024, 3, 5)) == 39
    assert is_stale_member(date(2023, 10, 17), date(2024, 1, 15)) is False
    assert is_stale_member(date(2023, 10, 16), date(2024, 1, 15)) is True


def test_tabular_country_files_and_collision_handling():
    pipe = SkyPointsPipeline(":memory:")
    batch = date(2022, 7, 1)

    pipe.ingest_tabular_file(str(DATA_DIR / "IND.csv"), batch_date=batch, seq_offset=0, as_of_date=date(2024, 1, 15))
    pipe.ingest_tabular_file(str(DATA_DIR / "USA.csv"), batch_date=batch, seq_offset=100, as_of_date=date(2024, 1, 15))
    pipe.ingest_tabular_file(str(DATA_DIR / "AUS.xlsx"), batch_date=batch, seq_offset=200, as_of_date=date(2024, 1, 15))
    pipe.route_to_country_tables(batch)

    aus_rows = pipe.conn.execute("SELECT * FROM TABLE_AUS ORDER BY member_id").fetchall()
    ind_rows = pipe.conn.execute("SELECT * FROM TABLE_IND").fetchall()
    usa_rows = pipe.conn.execute("SELECT * FROM TABLE_USA").fetchall()

    assert len(aus_rows) == 3
    assert len(ind_rows) == 0
    assert len(usa_rows) == 0

    dq_rows = pipe.conn.execute("SELECT * FROM DQ_REJECTED_RECORDS WHERE source_layer = 'STAGING_DEDUP'").fetchall()
    assert len(dq_rows) == 3


def test_member_country_move_evicts_old_country_table():
    pipe = SkyPointsPipeline(":memory:")

    pipe.ingest_tabular_file(str(DATA_DIR / "IND.csv"), batch_date=date(2022, 7, 1), as_of_date=date(2024, 1, 15))
    pipe.route_to_country_tables(date(2022, 7, 1))
    assert pipe.conn.execute("SELECT COUNT(*) FROM TABLE_IND").fetchone()[0] == 3

    pipe.ingest_tabular_file(str(DATA_DIR / "USA.csv"), batch_date=date(2022, 7, 2), as_of_date=date(2024, 1, 15))
    pipe.route_to_country_tables(date(2022, 7, 2))
    assert pipe.conn.execute("SELECT COUNT(*) FROM TABLE_IND").fetchone()[0] == 0
    assert pipe.conn.execute("SELECT COUNT(*) FROM TABLE_USA").fetchone()[0] == 3


def test_pipe_file_anomalies_and_redemption_join():
    pipe = SkyPointsPipeline(":memory:")
    batch = date(2024, 1, 15)

    pipe.ingest_pipe_file(str(DATA_DIR / "member_profile_20240115.dat"), batch_date=batch)
    pipe.route_to_country_tables(batch)
    pipe.ingest_redemption_json(str(DATA_DIR / "redemptions_20240115.json"))

    usa_elena = pipe.conn.execute("SELECT * FROM TABLE_USA WHERE member_id = '223457'").fetchone()
    assert usa_elena is not None
    assert usa_elena["member_name"] == "Elena"
    assert usa_elena["stale_member"] == 1

    dq_pipe = pipe.conn.execute("SELECT COUNT(*) FROM DQ_REJECTED_RECORDS WHERE source_layer = 'PIPE_FLAT_FILE'").fetchone()[0]
    assert dq_pipe == 4

    joined = pipe.get_enriched_redemptions()
    assert len(joined) == 2
    assert joined[0]["redemption_txn_id"] == "RX10091"
    assert joined[0]["member_name"] == "Elena"
    assert joined[0]["country_code"] == "USA"
