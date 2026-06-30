SELECT
  d.team_name,
  ds.year,
  COALESCE(SUM(ds.total_points), 0) AS total_points
FROM {{ ref("stg_driver_standings") }} AS ds
LEFT JOIN {{ ref}} AS d ON d.driver_number = ds.driver_number AND d.year = ds.year
GROUP BY d.team_name, ds.year
ORDER BY ds.year DESC, SUM(ds.total_points) DESC