SELECT
    CAST(driver_number AS INT64) AS driver_number,
    CAST(year AS INT64) AS year,
    COALESCE(SUM(points), 0) AS total_points
FROM {{ ref('stg_session_results_race') }}
GROUP BY driver_number, year
