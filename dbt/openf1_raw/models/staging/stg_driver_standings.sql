SELECT
    driver_number,
    year,
    COALESCE(SUM(points), 0) AS total_points
FROM {{ ref('stg_session_results_race') }}
GROUP BY driver_number, year
ORDER BY total_points DESC
