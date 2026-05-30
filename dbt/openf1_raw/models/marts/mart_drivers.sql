SELECT 
    driver_number,
    broadcast_name,
    full_name,
    name_acronym,
    team_name,
    team_colour, 
    first_name,
    last_name,
    headshot_url,
    country_code,
    year,
    CURRENT_TIMESTAMP() AS ingested_at
FROM {{ ref('stg_drivers') }}