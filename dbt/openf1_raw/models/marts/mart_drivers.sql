SELECT 
    d.driver_number,
    d.broadcast_name,
    d.full_name,
    d.name_acronym,
    d.team_name,
    d.team_colour, 
    d.first_name,
    d.last_name,
    d.headshot_url,
    d.country_code,
    d.year,
    COALESCE(ds.total_points, 0) AS total_points,
    CURRENT_TIMESTAMP() AS ingested_at
FROM {{ ref('stg_drivers') }} AS d
LEFT JOIN {{ ref('stg_driver_standings')}} AS ds
    ON d.driver_number = ds.driver_number AND d.year = ds.year