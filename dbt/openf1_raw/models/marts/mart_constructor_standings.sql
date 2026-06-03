SELECT
    team_name,
    SUM(total_points) AS total_points
FROM {{ ref("mart_drivers") }}
GROUP BY team_name
ORDER BY total_points DESC