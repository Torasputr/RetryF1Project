SELECT
    CAST(session_key AS INT64) AS session_key,
    TRIM(session_type) AS session_type,
    TRIM(session_name) AS session_name,
    CAST(date_start AS TIMESTAMP) AS date_start,
    CAST(date_end AS TIMESTAMP) AS date_end,
    CAST(meeting_key AS INT64) AS meeting_key,
    CAST(circuit_key AS INT64) AS circuit_key,
    TRIM(circuit_short_name) AS circuit_short_name,
    CAST(country_key AS INT64) AS country_key,
    TRIM(country_code) AS country_code,
    TRIM(country_name) AS country_name,
    TRIM(location) AS location,
    TRIM(gmt_offset) AS gmt_offset,
    CAST(year AS INT64) AS year,
    CAST(is_cancelled AS BOOL) AS is_cancelled,
    CURRENT_TIMESTAMP() AS ingested_at
FROM {{source("openf1_raw", "raw_sessions")}}