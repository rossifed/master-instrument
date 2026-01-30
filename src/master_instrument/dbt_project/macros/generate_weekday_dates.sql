{% macro generate_weekday_dates(start_date='2000-01-01', end_date='2050-12-31') %}
{#
    Generates a date series excluding weekends (Saturday/Sunday).
    
    Usage:
        {{ generate_weekday_dates() }}                           -- default range
        {{ generate_weekday_dates('2020-01-01', '2025-12-31') }} -- custom range
    
    Returns:
        - date: DATE (all weekdays in the range)
    
    Note: This is NOT a trading calendar - it simply excludes weekends.
    For actual trading calendars with holidays, replace with a proper calendar table.
#}
SELECT d::DATE AS date
FROM generate_series('{{ start_date }}'::date, '{{ end_date }}'::date, INTERVAL '1 day') AS d
WHERE EXTRACT(DOW FROM d) NOT IN (0, 6)  -- Exclude Saturday (6) and Sunday (0)
{% endmacro %}
