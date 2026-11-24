CREATE DATABASE IF NOT EXISTS SKYPOINTS_DW;
CREATE SCHEMA IF NOT EXISTS SKYPOINTS_DW.LOYALTY;
USE SCHEMA SKYPOINTS_DW.LOYALTY;

CREATE OR REPLACE TRANSIENT TABLE RAW_MEMBER_PROFILE (
    raw_record_id        STRING          NOT NULL,
    source_file_name     STRING          NOT NULL,
    file_batch_date      DATE            NOT NULL,
    ingested_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    record_type          STRING,
    raw_line             STRING,
    member_name_raw      STRING,
    member_id_raw        STRING,
    enrollment_date_raw  STRING,
    last_flight_date_raw STRING,
    tier_code_raw        STRING,
    agent_name_raw       STRING,
    member_segment_raw   STRING,
    state_raw            STRING,
    country_raw          STRING,
    post_code_raw        STRING,
    dob_raw              STRING,
    is_active_raw        STRING,
    record_seq_num       BIGINT          NOT NULL
)
CLUSTER BY (file_batch_date, record_type);

CREATE OR REPLACE TRANSIENT TABLE RAW_REDEMPTION_FEED (
    raw_event_id         STRING          NOT NULL,
    source_file_name     STRING          NOT NULL,
    ingested_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    payload              VARIANT         NOT NULL
)
CLUSTER BY (TO_DATE(payload:feed_date::STRING, 'YYYYMMDD'));

CREATE OR REPLACE TABLE STG_MEMBER_PROFILE (
    member_id            VARCHAR(18)     NOT NULL,
    member_name          VARCHAR(255)    NOT NULL,
    enrollment_date      DATE            NOT NULL,
    last_flight_date     DATE,
    tier_code            CHAR(5),
    agent_name           CHAR(255),
    member_segment       CHAR(1),
    state_code           CHAR(5),
    country_code         VARCHAR(10)     NOT NULL,
    post_code            INTEGER,
    date_of_birth        DATE,
    is_active_flag       CHAR(1),
    age_years            INTEGER,
    stale_member         BOOLEAN         NOT NULL DEFAULT FALSE,
    source_file_name     STRING          NOT NULL,
    file_batch_date      DATE            NOT NULL,
    record_seq_num       BIGINT          NOT NULL,
    record_hash          STRING          NOT NULL,
    loaded_at            TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_stg_member_batch UNIQUE (member_id, file_batch_date, record_seq_num)
)
CLUSTER BY (file_batch_date, country_code, member_id);

CREATE OR REPLACE TABLE FCT_MEMBER_REDEMPTIONS (
    redemption_txn_id    STRING          NOT NULL,
    member_id            VARCHAR(18)     NOT NULL,
    feed_date            DATE            NOT NULL,
    txn_date             DATE            NOT NULL,
    partner_name         STRING          NOT NULL,
    miles_redeemed       NUMBER(18, 2)   NOT NULL,
    redemption_status    STRING          NOT NULL,
    source_file_name     STRING          NOT NULL,
    ingested_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_fct_redemption_txn UNIQUE (redemption_txn_id)
)
CLUSTER BY (feed_date, member_id);

CREATE OR REPLACE TABLE TABLE_IND (
    member_id            VARCHAR(18)     NOT NULL,
    member_name          VARCHAR(255)    NOT NULL,
    enrollment_date      DATE            NOT NULL,
    last_flight_date     DATE,
    tier_code            CHAR(5),
    agent_name           CHAR(255),
    member_segment       CHAR(1),
    state_code           CHAR(5),
    country_code         VARCHAR(10)     NOT NULL,
    post_code            INTEGER,
    date_of_birth        DATE,
    is_active_flag       CHAR(1),
    age_years            INTEGER,
    stale_member         BOOLEAN         NOT NULL,
    effective_batch_date DATE            NOT NULL,
    record_hash          STRING          NOT NULL,
    updated_at           TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_table_ind PRIMARY KEY (member_id)
)
CLUSTER BY (member_id);

CREATE OR REPLACE TABLE TABLE_USA   LIKE TABLE_IND;
CREATE OR REPLACE TABLE TABLE_AUS   LIKE TABLE_IND;
CREATE OR REPLACE TABLE TABLE_PHIL  LIKE TABLE_IND;
CREATE OR REPLACE TABLE TABLE_CAN   LIKE TABLE_IND;
CREATE OR REPLACE TABLE TABLE_OTHER LIKE TABLE_IND;

CREATE OR REPLACE TABLE DQ_REJECTED_RECORDS (
    rejection_id         STRING          NOT NULL,
    source_layer         STRING          NOT NULL,
    source_file_name     STRING          NOT NULL,
    raw_record_key       STRING,
    failure_reasons      ARRAY           NOT NULL,
    raw_payload_snippet  STRING,
    rejected_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
