SELECT 
    s.meeting_key,
    m.meeting_name,
    m.meeting_official_name,
    s.session_key,
    s.session_type,
    s.session_name,
    s.date_start,
    s.date_end,
    s.country_key,
    s.country_code,
    s.country_name,
    m.country_flag,
    s.location,
    s.gmt_offset,
    s.circuit_key,
    s.circuit_short_name,
    m.circuit_type,
    m.circuit_info_url,
    m.circuit_image,
    s.year,
    s.is_cancelled,
    CURRENT_DATETIME() AS ingested_at
FROM {{ ref('stg_sessions') }} AS s
LEFT JOIN {{ref('stg_meetings')}} AS m
    ON s.meeting_key = m.meeting_key