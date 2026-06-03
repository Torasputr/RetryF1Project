WITH base AS (
    SELECT *
    FROM {{ source('openf1_raw', 'raw_session_results') }}
    WHERE session_type = 'Qualifying'
),

parsed AS (
    SELECT
        *,
        SPLIT(
            REGEXP_REPLACE(TRIM(duration), r'^\[|\]$', ''),
            ','
        ) AS duration_parts
    FROM base
),

with_quarters AS (
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
        TRIM(gap_to_leader) AS gap_to_leader,
        duration AS duration_raw,
        SAFE_CAST(position AS INT64) AS position,
        SAFE_CAST(points AS INT64) AS points,

        CASE
            WHEN duration IS NULL THEN NULL
            WHEN TRIM(duration_parts[SAFE_OFFSET(0)]) IN ('None', 'null', '') THEN NULL
            ELSE SAFE_CAST(TRIM(duration_parts[SAFE_OFFSET(0)]) AS FLOAT64)
        END AS q1,

        CASE
            WHEN duration IS NULL THEN NULL
            WHEN TRIM(duration_parts[SAFE_OFFSET(1)]) IN ('None', 'null', '') THEN NULL
            ELSE SAFE_CAST(TRIM(duration_parts[SAFE_OFFSET(1)]) AS FLOAT64)
        END AS q2,

        CASE
            WHEN duration IS NULL THEN NULL
            WHEN TRIM(duration_parts[SAFE_OFFSET(2)]) IN ('None', 'null', '') THEN NULL
            ELSE SAFE_CAST(TRIM(duration_parts[SAFE_OFFSET(2)]) AS FLOAT64)
        END AS q3,

        (
            SELECT MIN(t)
            FROM UNNEST([
                CASE
                    WHEN TRIM(duration_parts[SAFE_OFFSET(0)]) IN ('None', 'null', '') THEN NULL
                    ELSE SAFE_CAST(TRIM(duration_parts[SAFE_OFFSET(0)]) AS FLOAT64)
                END,
                CASE
                    WHEN TRIM(duration_parts[SAFE_OFFSET(1)]) IN ('None', 'null', '') THEN NULL
                    ELSE SAFE_CAST(TRIM(duration_parts[SAFE_OFFSET(1)]) AS FLOAT64)
                END,
                CASE
                    WHEN TRIM(duration_parts[SAFE_OFFSET(2)]) IN ('None', 'null', '') THEN NULL
                    ELSE SAFE_CAST(TRIM(duration_parts[SAFE_OFFSET(2)]) AS FLOAT64)
                END
            ]) AS t
            WHERE t IS NOT NULL
        ) AS best_lap_time

    FROM parsed
)

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
                best_lap_time ASC NULLS LAST,
                SAFE_CAST(driver_number AS INT64)
        )
    ) AS position,
    driver_number,
    number_of_laps,
    dnf,
    dns,
    dsq,
    gap_to_leader,
    duration_raw,
    q1,
    q2,
    q3,
    best_lap_time,
    points
FROM with_quarters