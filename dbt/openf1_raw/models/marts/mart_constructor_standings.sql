SELECT
    d.team_name,
    ds.year,
    SUM(ds.total_points) AS total_points
FROM {{ ref('mart_driver_standings') }} AS ds
INNER JOIN {{ ref('mart_drivers') }} AS d
    ON ds.driver_number = d.driver_number
    AND ds.year = d.year
GROUP BY d.team_name, ds.year
ORDER BY ds.year DESC, total_points DESC
