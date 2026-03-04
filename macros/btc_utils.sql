{% macro convert_to_usd(x,price) %}

    {{x}} * {{price}}
  
{% endmacro %}