{{ config (
    materialized="table"
)}}


with customers as (
    select
        id as customer_id,
        first_name,
        last_name
    from
        {{ source('shopping', 'raw_customers') }}
)

select * from customers
