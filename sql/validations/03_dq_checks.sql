-- Quarantine invalid flat-file member records into DQ_REJECTED_RECORDS
INSERT INTO DQ_REJECTED_RECORDS (
    rejection_id, source_layer, source_file_name, raw_record_key, failure_reasons, raw_payload_snippet
)
SELECT
    UUID_STRING(),
    'FLAT_FILE',
    source_file_name,
    COALESCE(TRIM(member_id_raw), raw_record_id),
    ARRAY_COMPACT(ARRAY_CONSTRUCT(
        IFF(TRIM(COALESCE(member_id_raw, '')) = '', 'MISSING_MEMBER_ID', NULL),
        IFF(LENGTH(TRIM(COALESCE(member_id_raw, ''))) > 18, 'MEMBER_ID_TOO_LONG', NULL),
        IFF(TRIM(COALESCE(member_name_raw, '')) = '', 'MISSING_MEMBER_NAME', NULL),
        IFF(
            COALESCE(
                TRY_TO_DATE(TRIM(enrollment_date_raw), 'YYYYMMDD'),
                TRY_TO_DATE(TRIM(enrollment_date_raw), 'MM/DD/YYYY')
            ) IS NULL,
            'INVALID_ENROLLMENT_DATE',
            NULL
        ),
        IFF(
            TRIM(COALESCE(dob_raw, '')) <> ''
            AND COALESCE(
                TRY_TO_DATE(LPAD(TRIM(dob_raw), 8, '0'), 'MMDDYYYY'),
                TRY_TO_DATE(TRIM(dob_raw), 'MM/DD/YYYY'),
                TRY_TO_DATE(TRIM(dob_raw), 'YYYYMMDD')
            ) IS NULL,
            'INVALID_DOB_FORMAT',
            NULL
        ),
        IFF(
            TRIM(COALESCE(member_segment_raw, '')) <> ''
            AND UPPER(TRIM(member_segment_raw)) NOT IN ('I', 'C'),
            'INVALID_MEMBER_SEGMENT',
            NULL
        ),
        IFF(
            COALESCE(
                TRY_TO_DATE(TRIM(last_flight_date_raw), 'YYYYMMDD'),
                TRY_TO_DATE(TRIM(last_flight_date_raw), 'MM/DD/YYYY')
            ) IS NOT NULL
            AND COALESCE(
                TRY_TO_DATE(TRIM(enrollment_date_raw), 'YYYYMMDD'),
                TRY_TO_DATE(TRIM(enrollment_date_raw), 'MM/DD/YYYY')
            ) IS NOT NULL
            AND COALESCE(
                TRY_TO_DATE(TRIM(last_flight_date_raw), 'YYYYMMDD'),
                TRY_TO_DATE(TRIM(last_flight_date_raw), 'MM/DD/YYYY')
            ) < COALESCE(
                TRY_TO_DATE(TRIM(enrollment_date_raw), 'YYYYMMDD'),
                TRY_TO_DATE(TRIM(enrollment_date_raw), 'MM/DD/YYYY')
            ),
            'FLIGHT_BEFORE_ENROLLMENT',
            NULL
        )
    )),
    LEFT(COALESCE(raw_line, OBJECT_CONSTRUCT(*)::STRING), 1000)
FROM RAW_MEMBER_PROFILE
WHERE record_type = 'D'
  AND (
      TRIM(COALESCE(member_id_raw, '')) = ''
      OR LENGTH(TRIM(COALESCE(member_id_raw, ''))) > 18
      OR TRIM(COALESCE(member_name_raw, '')) = ''
      OR COALESCE(
          TRY_TO_DATE(TRIM(enrollment_date_raw), 'YYYYMMDD'),
          TRY_TO_DATE(TRIM(enrollment_date_raw), 'MM/DD/YYYY')
      ) IS NULL
      OR (
          TRIM(COALESCE(dob_raw, '')) <> ''
          AND COALESCE(
              TRY_TO_DATE(LPAD(TRIM(dob_raw), 8, '0'), 'MMDDYYYY'),
              TRY_TO_DATE(TRIM(dob_raw), 'MM/DD/YYYY'),
              TRY_TO_DATE(TRIM(dob_raw), 'YYYYMMDD')
          ) IS NULL
      )
      OR (
          TRIM(COALESCE(member_segment_raw, '')) <> ''
          AND UPPER(TRIM(member_segment_raw)) NOT IN ('I', 'C')
      )
      OR (
          COALESCE(
              TRY_TO_DATE(TRIM(last_flight_date_raw), 'YYYYMMDD'),
              TRY_TO_DATE(TRIM(last_flight_date_raw), 'MM/DD/YYYY')
          ) IS NOT NULL
          AND COALESCE(
              TRY_TO_DATE(TRIM(enrollment_date_raw), 'YYYYMMDD'),
              TRY_TO_DATE(TRIM(enrollment_date_raw), 'MM/DD/YYYY')
          ) IS NOT NULL
          AND COALESCE(
              TRY_TO_DATE(TRIM(last_flight_date_raw), 'YYYYMMDD'),
              TRY_TO_DATE(TRIM(last_flight_date_raw), 'MM/DD/YYYY')
          ) < COALESCE(
              TRY_TO_DATE(TRIM(enrollment_date_raw), 'YYYYMMDD'),
              TRY_TO_DATE(TRIM(enrollment_date_raw), 'MM/DD/YYYY')
          )
      )
  );
