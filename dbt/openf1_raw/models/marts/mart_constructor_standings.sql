SELECT
    team_name,
    year,
    SUM(total_points) AS total_points
FROM {{ ref("mart_drivers") }}
GROUP BY team_name, year
ORDER BY year DESC, total_points DESC