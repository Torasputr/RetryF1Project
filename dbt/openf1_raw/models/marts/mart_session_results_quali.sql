SELECT 
    meeting_key,
    session_key,
    session_type,
    session_name,
    date_start,
    year,
    driver_number,
    number_of_laps,
    dnf,
    dns,
    dsq,
    q1,
    q2,
    q3,
    best_lap_time,
    position,
FROM {{ ref("stg_session_results_qualifying") }}
ORDER BY session_key ASC, position ASC