{{ config(materialized='table') }}

{% if execute %}
    {% set dq_query %}
        with customer_orders as (
            select
                o.order_id,
                o.order_purchase_timestamp,
                c.customer_unique_id,
                c.customer_zip_code_prefix,
                c.customer_city,
                c.customer_state
            from {{ ref('stg_olist_orders') }} o
            inner join {{ ref('stg_olist_customers') }} c
                on o.customer_id = c.customer_id
        ),
        ordered_addresses as (
            select
                *,
                lag(customer_unique_id) over (
                    partition by customer_unique_id
                    order by order_purchase_timestamp, order_id
                ) as previous_customer_unique_id,
                lag(customer_zip_code_prefix) over (
                    partition by customer_unique_id
                    order by order_purchase_timestamp, order_id
                ) as previous_zip_code_prefix,
                lag(customer_city) over (
                    partition by customer_unique_id
                    order by order_purchase_timestamp, order_id
                ) as previous_city,
                lag(customer_state) over (
                    partition by customer_unique_id
                    order by order_purchase_timestamp, order_id
                ) as previous_state
            from customer_orders
        ),
        versioned_orders as (
            select
                *,
                sum(
                    case
                        when previous_customer_unique_id is null
                            or customer_zip_code_prefix IS DISTINCT FROM previous_zip_code_prefix
                            or customer_city IS DISTINCT FROM previous_city
                            or customer_state IS DISTINCT FROM previous_state
                        then 1
                        else 0
                    end
                ) over (
                    partition by customer_unique_id
                    order by order_purchase_timestamp, order_id
                    rows unbounded preceding
                ) as version_number
            from ordered_addresses
        ),
        version_counts as (
            select
                customer_unique_id,
                count(distinct version_number) as version_count
            from versioned_orders
            group by customer_unique_id
        )
        select
            count_if(version_count > 1) as customers_with_address_changes,
            count(*) as total_customers
        from version_counts
    {% endset %}

    {% set dq_result = run_query(dq_query) %}
    {% if dq_result and dq_result.rows %}
        {% set customers_with_address_changes = dq_result.columns[0].values()[0] | int %}
        {% set total_customers = dq_result.columns[1].values()[0] | int %}
        {% set change_percentage = (100.0 * customers_with_address_changes / total_customers) if total_customers else 0.0 %}
        {{ log(customers_with_address_changes ~ ' of ' ~ total_customers ~ ' unique customers (' ~ ('%.2f' | format(change_percentage)) ~ '%) show 2+ distinct address versions', info=True) }}
    {% endif %}
{% endif %}

with customer_orders as (
    select
        o.order_id,
        o.order_purchase_timestamp,
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state
    from {{ ref('stg_olist_orders') }} o
    inner join {{ ref('stg_olist_customers') }} c
        on o.customer_id = c.customer_id
),

ordered_addresses as (
    select
        *,
        lag(customer_unique_id) over (
            partition by customer_unique_id
            order by order_purchase_timestamp, order_id
        ) as previous_customer_unique_id,
        lag(customer_zip_code_prefix) over (
            partition by customer_unique_id
            order by order_purchase_timestamp, order_id
        ) as previous_zip_code_prefix,
        lag(customer_city) over (
            partition by customer_unique_id
            order by order_purchase_timestamp, order_id
        ) as previous_city,
        lag(customer_state) over (
            partition by customer_unique_id
            order by order_purchase_timestamp, order_id
        ) as previous_state
    from customer_orders
),

versioned_orders as (
    select
        *,
        sum(
            case
                when previous_customer_unique_id is null
                    or customer_zip_code_prefix IS DISTINCT FROM previous_zip_code_prefix
                    or customer_city IS DISTINCT FROM previous_city
                    or customer_state IS DISTINCT FROM previous_state
                then 1
                else 0
            end
        ) over (
            partition by customer_unique_id
            order by order_purchase_timestamp, order_id
            rows unbounded preceding
        ) as version_number
    from ordered_addresses
),

version_starts as (
    select
        customer_unique_id,
        version_number,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        min(order_purchase_timestamp) as valid_from
    from versioned_orders
    group by
        customer_unique_id,
        version_number,
        customer_zip_code_prefix,
        customer_city,
        customer_state
)

, version_ranges as (
    select
        *,
        lead(valid_from) over (
            partition by customer_unique_id
            order by version_number
        ) as valid_to
    from version_starts
)

select
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    valid_from,
    valid_to,
    valid_to is null as is_current,
    version_number
from version_ranges