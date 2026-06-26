SELECT
  SAFE_CAST(year AS INT64) AS year,
  SAFE_CAST(driver_number AS INT64) AS driver_number,
  TRIM(broadcast_name) AS broadcast_name,
  TRIM(full_name) AS full_name,
  TRIM(name_acronym) AS name_acronym,
  TRIM(team_name) AS team_name,
  TRIM(team_colour) AS team_colour,
  TRIM(first_name) AS first_name,
  TRIM(last_name) AS last_name,
  TRIM(headshot_url) AS headshot_url,
  CASE TRIM(name_acronym)
    WHEN 'CRA' THEN 'USA'
    WHEN 'BEG' THEN 'SUI'
    WHEN 'ARO' THEN 'EST'
    WHEN 'VES' THEN 'DEN'
    WHEN 'HER' THEN 'USA'
    WHEN 'IWA' THEN 'JPN'
    WHEN 'FOR' THEN 'ITA'
    WHEN 'NOR' THEN 'GBR'
    WHEN 'VER' THEN 'NLD'
    WHEN 'BOR' THEN 'BRA'
    WHEN 'HAD' THEN 'FRA'
    WHEN 'GAS' THEN 'FRA'
    WHEN 'PER' THEN 'MEX'
    WHEN 'ANT' THEN 'ITA'
    WHEN 'ALO' THEN 'ESP'
    WHEN 'LEC' THEN 'MON'
    WHEN 'STR' THEN 'CAN'
    WHEN 'ALB' THEN 'THA'
    WHEN 'HUL' THEN 'GER'
    WHEN 'LAW' THEN 'NZL'
    WHEN 'OCO' THEN 'FRA'
    WHEN 'LIN' THEN 'GBR'
    WHEN 'COL' THEN 'ARG'
    WHEN 'HAM' THEN 'GBR'
    WHEN 'SAI' THEN 'ESP'
    WHEN 'RUS' THEN 'GBR'
    WHEN 'BOT' THEN 'FIN'
    WHEN 'PIA' THEN 'AUS'
    WHEN 'BEA' THEN 'GBR'
    WHEN 'HIR' THEN 'JPN'
    WHEN 'DOO' THEN 'AUS'
    WHEN 'MAR' THEN 'FRA'
    WHEN 'FUO' THEN 'ITA'
    WHEN 'SHI' THEN 'IRL'
    WHEN 'LEL' THEN 'MON'
    WHEN 'BRO' THEN 'GBR'
    WHEN 'OWA' THEN 'USA'
    WHEN 'TSU' THEN 'JPN'
  END AS country_code,
  CURRENT_TIMESTAMP() AS ingested_at
FROM {{ source("openf1_raw", "raw_drivers")}}