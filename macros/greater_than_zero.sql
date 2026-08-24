{% test greater_than_zero(model, column_name) %}

-- Generic Test: Verify that a numeric column has values strictly greater than zero.
-- This catches both NULL values and invalid 0 or negative quantities.

select
    {{ column_name }} as invalid_value
from {{ model }}
where {{ column_name }} <= 0 or {{ column_name }} is null

{% endtest %}
