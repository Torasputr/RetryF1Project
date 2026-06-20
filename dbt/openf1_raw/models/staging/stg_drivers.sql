-- SELECT 

--     CAST(meeting_key AS INT64) AS meeting_key,
--     CAST(session_key AS INT64) AS session_key,
--     CAST(driver_number AS INT64) AS driver_number,
--     TRIM(broadcast_name) AS broadcast_name,
--     TRIM(full_name) AS full_name,
--     TRIM(name_acronym) AS name_acronym,
--     TRIM(team_name) AS team_name,
--     TRIM(team_colour) AS team_colour,
--     TRIM(first_name) AS first_name,
--     TRIM(last_name) AS last_name,
--     TRIM(headshot_url) AS headshot_url,
--     CASE name_acronym
--         WHEN 'GAS' THEN 'FRA'
--         WHEN 'COL' THEN 'ARG'
--         WHEN 'ANT' THEN 'ITA'
--         WHEN 'RUS' THEN 'GBR'
--         WHEN 'ALB' THEN 'THA'
--         WHEN 'SAI' THEN 'ESP'
--         WHEN 'ALO' THEN 'ESP'
--         WHEN 'STR' THEN 'CAN'
--         WHEN 'VER' THEN 'NLD'
--         WHEN 'HAD' THEN 'FRA'
--         WHEN 'LAW' THEN 'NZL'
--         WHEN 'LIN' THEN 'GBR'
--         WHEN 'PER' THEN 'MEX'
--         WHEN 'BOT' THEN 'FIN'
--         WHEN 'OCO' THEN 'FRA'
--         WHEN 'BEA' THEN 'GBR'
--         WHEN 'LEC' THEN 'MON'
--         WHEN 'HAM' THEN 'GBR'
--         WHEN 'NOR' THEN 'GBR'
--         WHEN 'PIA' THEN 'AUS'
--         WHEN 'BOR' THEN 'BRA'
--         WHEN 'HUL' THEN 'GER'
--         WHEN 'TSU' THEN 'JPN'
--     END AS country_code,
--     CAST(year AS INT64) AS year,
--     CURRENT_TIMESTAMP() AS ingested_at
-- FROM {{ source('openf1_raw', 'raw_drivers')}}

WITH race_count AS (
    SELECT
        SAFE_CAST(driver_number AS INT64) AS driver_number,
        CAST(year AS INT64) AS year,
        COUNT(*) AS race_count
    FROM {{ source('openf1_raw', 'raw_session_results') }}
    WHERE TRIM(session_name) = 'Race'
    GROUP BY driver_number, year
),

driver_raw AS (
  SELECT 
    DISTINCT(r.driver_number),
    r.year,
    COALESCE(rc.race_count, 0) AS race_count
  FROM {{ source('openf1_raw', 'raw_session_results') }} AS r
  LEFT JOIN race_count AS rc ON rc.driver_number = r.driver_number AND rc.year = r.year
)

SELECT
  ROW_NUMBER () OVER () AS driver_id,
  CAST(r.driver_number AS INT64) AS driver_number,
  CAST(r.year AS INT64) AS year,
  TRIM(d.broadcast_name) AS broadcast_name,
  TRIM(d.full_name) AS full_name,
  TRIM(d.name_acronym) AS name_acronym,
  TRIM(d.team_name) AS team_name,
  TRIM(d.team_colour) AS team_colour,
  TRIM(d.first_name) AS first_name,
  TRIM(d.last_name) AS last_name,
  TRIM(d.headshot_url) AS headshot_url,
  CASE name_acronym
        WHEN 'GAS' THEN 'FRA'
        WHEN 'COL' THEN 'ARG'
        WHEN 'ANT' THEN 'ITA'
        WHEN 'RUS' THEN 'GBR'
        WHEN 'ALB' THEN 'THA'
        WHEN 'SAI' THEN 'ESP'
        WHEN 'ALO' THEN 'ESP'
        WHEN 'STR' THEN 'CAN'
        WHEN 'VER' THEN 'NLD'
        WHEN 'HAD' THEN 'FRA'
        WHEN 'LAW' THEN 'NZL'
        WHEN 'LIN' THEN 'GBR'
        WHEN 'PER' THEN 'MEX'
        WHEN 'BOT' THEN 'FIN'
        WHEN 'OCO' THEN 'FRA'
        WHEN 'BEA' THEN 'GBR'
        WHEN 'LEC' THEN 'MON'
        WHEN 'HAM' THEN 'GBR'
        WHEN 'NOR' THEN 'GBR'
        WHEN 'PIA' THEN 'AUS'
        WHEN 'BOR' THEN 'BRA'
        WHEN 'HUL' THEN 'GER'
        WHEN 'TSU' THEN 'JPN'
    END AS country_code,
  CAST(r.race_count AS INT64) AS race_count,
  CURRENT_TIMESTAMP() AS ingested_at
FROM driver_raw AS r
LEFT JOIN {{ source('openf1_raw', 'raw_drivers') }} AS d ON r.driver_number = d.driver_number AND r.year = d.year 