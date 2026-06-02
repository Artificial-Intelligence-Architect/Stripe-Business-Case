{% macro convert_to_usd(amount, currency) %}
    case
        when {{ currency }} = 'USD' then {{ amount }}
        when {{ currency }} = 'EUR' then {{ amount }} * 1.10
        when {{ currency }} = 'GBP' then {{ amount }} * 1.27
        else {{ amount }}
    end
{% endmacro %}