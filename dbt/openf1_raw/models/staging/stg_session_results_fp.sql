SELECT
    SAFE_CAST(meeting_key AS INT64) AS meeting_key,
    SAFE_CAST(session_key AS INT64) AS session_key,
    TRIM(session_type) AS session_type,
    TRIM(session_name) AS session_name,
    SAFE_CAST(date_start AS TIMESTAMP) AS date_start,
    SAFE_CAST(year AS INT64) AS year,
    SAFE_CAST(driver_number AS INT64) AS driver_number,
    SAFE_CAST(number_of_laps AS INT64) AS number_of_laps,
    SAFE_CAST(dnf AS BOOL) AS dnf,
    SAFE_CAST(dns AS BOOL) AS dns,
    SAFE_CAST(dsq AS BOOL) AS dsq,
    SAFE_CAST(gap_to_leader AS FLOAT64) AS gap_to_leader,
    SAFE_CAST(duration AS FLOAT64) AS duration,
    SAFE_CAST(position AS INT64) AS position,
    CURRENT_TIMESTAMP() AS ingested_at
FROM {{ source("openf1_raw", "raw_session_results") }}
WHERE session_type = "Practice"