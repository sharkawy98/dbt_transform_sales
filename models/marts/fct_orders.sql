{{ config(
    materialized='table'
)}}

select 
    l.order_id,
    l.customer_id,
    r.price
from
    {{ ref('stg_orders') }} l
join
    {{ ref('stg_payments') }} r
on
    l.order_id = r.order_id
