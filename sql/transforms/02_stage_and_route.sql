-- 1. Load Staging with Age & Stale_Member derivations
INSERT INTO STG_MEMBER_PROFILE (
    member_id, member_name, enrollment_date, last_flight_date,
    tier_code, agent_name, member_segment, state_code, country_code,
    post_code, date_of_birth, is_active_flag, age_years, stale_member,
    source_file_name, file_batch_date, record_seq_num, record_hash
)
WITH parsed AS (
    SELECT
        TRIM(member_id_raw)                                       AS member_id,
        TRIM(member_name_raw)                                     AS member_name,
        COALESCE(
            TRY_TO_DATE(TRIM(enrollment_date_raw), 'YYYYMMDD'),
            TRY_TO_DATE(TRIM(enrollment_date_raw), 'MM/DD/YYYY')
        )                                                         AS enrollment_date,
        COALESCE(
            TRY_TO_DATE(TRIM(last_flight_date_raw), 'YYYYMMDD'),
            TRY_TO_DATE(TRIM(last_flight_date_raw), 'MM/DD/YYYY')
        )                                                         AS last_flight_date,
        NULLIF(UPPER(TRIM(tier_code_raw)), '')                    AS tier_code,
        NULLIF(TRIM(agent_name_raw), '')                          AS agent_name,
        NULLIF(UPPER(TRIM(member_segment_raw)), '')               AS member_segment,
        NULLIF(UPPER(TRIM(state_raw)), '')                        AS state_code,
        COALESCE(NULLIF(UPPER(TRIM(country_raw)), ''), 'UNK')     AS country_code,
        TRY_TO_NUMBER(TRIM(post_code_raw))::INTEGER               AS post_code,
        COALESCE(
            TRY_TO_DATE(LPAD(TRIM(dob_raw), 8, '0'), 'MMDDYYYY'),
            TRY_TO_DATE(TRIM(dob_raw), 'MM/DD/YYYY'),
            TRY_TO_DATE(TRIM(dob_raw), 'YYYYMMDD')
        )                                                         AS date_of_birth,
        NULLIF(UPPER(TRIM(is_active_raw)), '')                    AS is_active_flag,
        source_file_name,
        file_batch_date,
        record_seq_num
    FROM RAW_MEMBER_PROFILE
    WHERE record_type = 'D'
)
SELECT
    member_id,
    member_name,
    enrollment_date,
    last_flight_date,
    tier_code,
    agent_name,
    member_segment,
    state_code,
    country_code,
    post_code,
    date_of_birth,
    is_active_flag,
    CASE
        WHEN date_of_birth IS NULL THEN NULL
        ELSE YEAR(CURRENT_DATE()) - YEAR(date_of_birth)
             - IFF((MONTH(date_of_birth) * 100 + DAY(date_of_birth)) >
                   (MONTH(CURRENT_DATE()) * 100 + DAY(CURRENT_DATE())), 1, 0)
    END                                                           AS age_years,
    IFF(last_flight_date IS NOT NULL AND DATEDIFF('day', last_flight_date, file_batch_date) > 90, TRUE, FALSE)
                                                                  AS stale_member,
    source_file_name,
    file_batch_date,
    record_seq_num,
    SHA2(OBJECT_CONSTRUCT(
        'member_id', member_id, 'member_name', member_name,
        'enrollment_date', TO_VARCHAR(enrollment_date), 'last_flight_date', TO_VARCHAR(last_flight_date),
        'tier_code', tier_code, 'agent_name', agent_name, 'member_segment', member_segment,
        'state_code', state_code, 'country_code', country_code, 'post_code', post_code,
        'date_of_birth', TO_VARCHAR(date_of_birth), 'is_active_flag', is_active_flag
    )::STRING, 256)                                               AS record_hash
FROM parsed
WHERE member_id <> '' AND member_name <> '' AND enrollment_date IS NOT NULL;

-- 2. Deduplicate using "latest record wins" per member_id
CREATE OR REPLACE TEMP TABLE TMP_LATEST_MEMBERS AS
SELECT
    member_id,
    member_name,
    enrollment_date,
    last_flight_date,
    tier_code,
    agent_name,
    member_segment,
    state_code,
    country_code,
    post_code,
    date_of_birth,
    is_active_flag,
    age_years,
    stale_member,
    file_batch_date AS effective_batch_date,
    record_hash
FROM STG_MEMBER_PROFILE
WHERE file_batch_date = :target_batch_date
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY member_id
    ORDER BY file_batch_date DESC, record_seq_num DESC, loaded_at DESC
) = 1;

-- 3. Upsert into country target table (example: TABLE_IND) & evict members who moved away
MERGE INTO TABLE_IND AS tgt
USING (
    SELECT * FROM TMP_LATEST_MEMBERS WHERE country_code = 'IND'
) AS src
ON tgt.member_id = src.member_id
WHEN MATCHED AND tgt.record_hash <> src.record_hash THEN
    UPDATE SET
        tgt.member_name          = src.member_name,
        tgt.enrollment_date      = src.enrollment_date,
        tgt.last_flight_date     = src.last_flight_date,
        tgt.tier_code            = src.tier_code,
        tgt.agent_name           = src.agent_name,
        tgt.member_segment       = src.member_segment,
        tgt.state_code           = src.state_code,
        tgt.country_code         = src.country_code,
        tgt.post_code            = src.post_code,
        tgt.date_of_birth        = src.date_of_birth,
        tgt.is_active_flag       = src.is_active_flag,
        tgt.age_years            = src.age_years,
        tgt.stale_member         = src.stale_member,
        tgt.effective_batch_date = src.effective_batch_date,
        tgt.record_hash          = src.record_hash,
        tgt.updated_at           = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        member_id, member_name, enrollment_date, last_flight_date,
        tier_code, agent_name, member_segment, state_code, country_code,
        post_code, date_of_birth, is_active_flag, age_years, stale_member,
        effective_batch_date, record_hash
    )
    VALUES (
        src.member_id, src.member_name, src.enrollment_date, src.last_flight_date,
        src.tier_code, src.agent_name, src.member_segment, src.state_code, src.country_code,
        src.post_code, src.date_of_birth, src.is_active_flag, src.age_years, src.stale_member,
        src.effective_batch_date, src.record_hash
    );

DELETE FROM TABLE_IND tgt
WHERE EXISTS (
    SELECT 1
    FROM TMP_LATEST_MEMBERS latest
    WHERE latest.member_id = tgt.member_id
      AND latest.country_code <> 'IND'
);

-- 4. Flatten JSON Redemptions & Join View
MERGE INTO FCT_MEMBER_REDEMPTIONS AS tgt
USING (
    SELECT
        r.value:txn_id::STRING                               AS redemption_txn_id,
        TRIM(payload:"member id"::STRING)                    AS member_id,
        TRY_TO_DATE(payload:feed_date::STRING, 'YYYYMMDD')   AS feed_date,
        TRY_TO_DATE(r.value:txn_date::STRING, 'YYYYMMDD')    AS txn_date,
        TRIM(r.value:partner::STRING)                        AS partner_name,
        r.value:miles_redeemed::NUMBER(18, 2)                AS miles_redeemed,
        UPPER(TRIM(r.value:status::STRING))                  AS redemption_status,
        raw.source_file_name,
        raw.ingested_at
    FROM RAW_REDEMPTION_FEED raw,
         LATERAL FLATTEN(input => raw.payload:redemptions) r
    WHERE r.value:txn_id::STRING IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY r.value:txn_id::STRING
        ORDER BY TRY_TO_DATE(payload:feed_date::STRING, 'YYYYMMDD') DESC, raw.ingested_at DESC
    ) = 1
) AS src
ON tgt.redemption_txn_id = src.redemption_txn_id
WHEN MATCHED THEN UPDATE SET
    tgt.member_id = src.member_id, tgt.feed_date = src.feed_date, tgt.txn_date = src.txn_date,
    tgt.partner_name = src.partner_name, tgt.miles_redeemed = src.miles_redeemed,
    tgt.redemption_status = src.redemption_status, tgt.source_file_name = src.source_file_name,
    tgt.ingested_at = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    redemption_txn_id, member_id, feed_date, txn_date,
    partner_name, miles_redeemed, redemption_status, source_file_name, ingested_at
) VALUES (
    src.redemption_txn_id, src.member_id, src.feed_date, src.txn_date,
    src.partner_name, src.miles_redeemed, src.redemption_status, src.source_file_name, src.ingested_at
);

CREATE OR REPLACE VIEW VW_ALL_MEMBERS_CURRENT AS
SELECT * FROM TABLE_IND UNION ALL
SELECT * FROM TABLE_USA UNION ALL
SELECT * FROM TABLE_AUS UNION ALL
SELECT * FROM TABLE_PHIL UNION ALL
SELECT * FROM TABLE_CAN UNION ALL
SELECT * FROM TABLE_OTHER;
