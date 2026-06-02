/*
  Fallback surrogate key macro — used when dbt_utils is not installed.
  If dbt_utils is available (packages.yml), this macro is overridden
  by dbt_utils.generate_surrogate_key automatically.

  Usage:
    {{ generate_surrogate_key(['col1', 'col2']) }}
    → MD5(col1 || '-' || col2)
*/

{% macro generate_surrogate_key(field_list) %}

    md5(
        {%- for field in field_list %}
            coalesce(cast({{ field }} as varchar), 'NULL')
            {%- if not loop.last %} || '-' || {% endif %}
        {%- endfor %}
    )

{% endmacro %}
