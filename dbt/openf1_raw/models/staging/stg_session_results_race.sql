SELECT
    meeting_key,
    session_key,
    session_type,
    session_name,
    date_start,
    year,
    COALESCE(
        SAFE_CAST(position AS INT64),
        ROW_NUMBER() OVER (
            PARTITION BY session_key
            ORDER BY
                SAFE_CAST(number_of_laps AS INT64) DESC NULLS LAST,
                SAFE_CAST(driver_number AS INT64)
            )
    ) AS position,
    SAFE_CAST(driver_number AS INT64) AS driver_number,
    SAFE_CAST(number_of_laps AS INT64) AS number_of_laps,
    SAFE_CAST(dnf AS BOOL) AS dnf,
    SAFE_CAST(dns AS BOOL) AS dns,
    SAFE_CAST(dsq AS BOOL) AS dsq,
    TRIM(gap_to_leader) AS gap_to_leader,
    SAFE_CAST(duration AS FLOAT64) AS duration,
    SAFE_CAST(points AS INT64) AS points
FROM {{ source('openf1_raw', 'raw_session_results') }}
WHERE session_type = 'Race'
ORDER BY session_key ASC, position ASC