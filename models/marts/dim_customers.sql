{{ config (
    materialized="table"
)}}


with customers as (
    select * from {{ ref('stg_customers')}}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customer_orders as (

    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as orders_count
    from orders
    group by 1
),

final as (
    select
        l.customer_id,
        l.first_name,
        l.last_name,
        r.first_order_date,
        r.most_recent_order_date,
        coalesce(r.orders_count, 0) as orders_count
    from customers l
    left join 
        customer_orders r using (customer_id)
    order by
        r.orders_count desc
)

select * from final
