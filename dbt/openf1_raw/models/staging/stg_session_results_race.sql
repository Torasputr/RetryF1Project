WITH base AS (
    SELECT
        meeting_key,
        session_key,
        session_type,
        TRIM(session_name) AS session_name,
        date_start,
        year,
        SAFE_CAST(position AS FLOAT64) AS position_num,
        SAFE_CAST(driver_number AS INT64) AS driver_number,
        SAFE_CAST(number_of_laps AS INT64) AS number_of_laps,
        SAFE_CAST(dnf AS BOOL) AS dnf,
        SAFE_CAST(dns AS BOOL) AS dns,
        SAFE_CAST(dsq AS BOOL) AS dsq,
        TRIM(CAST(gap_to_leader AS STRING)) AS gap_to_leader,
        SAFE_CAST(duration AS FLOAT64) AS duration,
        SAFE_CAST(points AS FLOAT64) AS raw_points
    FROM {{ source('openf1_raw', 'raw_session_results') }}
    WHERE session_type = 'Race'
        AND TRIM(session_name) IN ('Race', 'Sprint')
),

ranked AS (
    SELECT
        *,
        NULLIF(
            SAFE_CAST(ROUND(position_num) AS INT64),
            0
        ) AS parsed_position
    FROM base
),

with_position AS (
    SELECT
        meeting_key,
        session_key,
        session_type,
        session_name,
        date_start,
        year,
        driver_number,
        number_of_laps,
        dnf,
        dns,
        dsq,
        gap_to_leader,
        duration,
        raw_points,
        COALESCE(
            parsed_position,
            ROW_NUMBER() OVER (
                PARTITION BY session_key
                ORDER BY
                    number_of_laps DESC NULLS LAST,
                    driver_number
            )
        ) AS position
    FROM ranked
)

SELECT
    meeting_key,
    session_key,
    session_type,
    session_name,
    date_start,
    year,
    position,
    driver_number,
    number_of_laps,
    dnf,
    dns,
    dsq,
    gap_to_leader,
    duration,
    COALESCE(SAFE_CAST(raw_points AS INT64), 0) AS points
FROM with_position
ORDER BY session_key ASC, position ASC
