{% macro get_conversion_factor(value, converted_unit, item_precision) %}
    CASE
        -- Returns the conversion factor applied (for transparency/debugging)
        -- Only convert if item_precision is 1 or 2 (Per Share or Per Employee)
        WHEN {{ converted_unit }} = 'T' AND {{ item_precision }} IN (1, 2) 
            AND ABS({{ value }}) < 1e13 THEN 1000
        WHEN {{ converted_unit }} = 'M' AND {{ item_precision }} IN (1, 2) 
            AND ABS({{ value }}) < 1e10 THEN 1000000
        WHEN {{ converted_unit }} = 'B' AND {{ item_precision }} IN (1, 2) 
            AND ABS({{ value }}) < 1e7 THEN 1000000000
        ELSE 1  -- No conversion applied
    END
{% endmacro %}

{% macro convert_financial_value_units(value, converted_unit, item_precision) %}
    {{ value }} * {{ get_conversion_factor(value, converted_unit, item_precision) }}
{% endmacro %}
