{% macro f1_championship_points(session_name, position) %}
    CASE
        WHEN {{ position }} IS NULL OR {{ position }} < 1 THEN 0
        WHEN TRIM({{ session_name }}) = 'Sprint' AND {{ position }} BETWEEN 1 AND 8 THEN
            ([8, 7, 6, 5, 4, 3, 2, 1])[SAFE_OFFSET({{ position }} - 1)]
        WHEN {{ position }} BETWEEN 1 AND 10 THEN
            ([25, 18, 15, 12, 10, 8, 6, 4, 2, 1])[SAFE_OFFSET({{ position }} - 1)]
        ELSE 0
    END
{% endmacro %}
