{% macro clean_pt_vat(col) -%}
  CASE
    WHEN {{ col }} IS NULL THEN NULL
    ELSE
      'PT'
      || REGEXP_REPLACE(
           {{ col }},
           r'(?i)^(PT)|\s+',
           ''
         )
  END
{%- endmacro %}