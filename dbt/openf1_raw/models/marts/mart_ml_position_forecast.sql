SELECT
  r.meeting_key,
  r.driver_number,
  r.year,
  r.session_name,
  q.position AS grid_position,
  q.best_lap_time,
  r.position AS finish_position
FROM {{ref("stg_session_results_race")}} AS r
LEFT JOIN {{ref("stg_session_results_qualifying")}} AS q
  ON r.meeting_key = q.meeting_key
 AND r.driver_number = q.driver_number
 AND r.year = q.year
 AND q.session_name = CASE r.session_name
   WHEN 'Race' THEN 'Qualifying'
   WHEN 'Sprint' THEN 'Sprint Qualifying' 
 END
WHERE r.session_name IN ('Race', 'Sprint')