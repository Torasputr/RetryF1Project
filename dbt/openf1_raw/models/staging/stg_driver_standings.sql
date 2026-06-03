SELECT
    d.driver_number,
    SUM(srr.points) AS total_points
FROM {{ ref("stg_drivers") }} AS d
LEFT JOIN {{ref("stg_session_results_race")}} AS srr
    ON d.driver_number = srr.driver_number
GROUP BY driver_number
ORDER BY total_points DESC