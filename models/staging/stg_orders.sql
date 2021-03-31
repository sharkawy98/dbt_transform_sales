{{ config (
    materialized="table"
)}}


with orders as (
    select 
        id as order_id,
        user_id as customer_id,
        order_date,
        status
    from
        {{ source('shopping', 'raw_orders') }}
)

select * from orders
