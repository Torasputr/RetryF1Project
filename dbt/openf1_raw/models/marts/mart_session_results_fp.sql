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
    gap_to_leader,
    duration,
    position,
    ingested_at
FROM {{ ref("stg_session_results_fp") }}