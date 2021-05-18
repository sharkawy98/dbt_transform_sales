# dbt cloud project for sales dataset

## I made data transformations in Google BigQuery warehouse to apply the newly introduced ELT process:

**1. Produced some staging, fact and dimension tables. And here is the final lineage graph.**

![lineage graph](https://user-images.githubusercontent.com/36075516/118669944-781df880-b7f6-11eb-9b8d-73bc085cdb45.png)

**2. This is an example of applied queries.**

```
with customers as (
    select * from {{ ref('stg_customers')}}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

customer_orders as(
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as orders_count,
        sum(price) as total_spent,
    from
        {{ ref('fct_orders') }}
    group by 1
),

final as (
    select
        a.customer_id,
        a.first_name,
        a.last_name,
        b.first_order_date,
        b.most_recent_order_date,
        coalesce(b.orders_count, 0) as orders_count,
        coalesce(b.total_spent, 0) as total_spent
    from customers a
    
    -- left join to include customers who didn't buy anything
    left join 
        customer_orders b using(customer_id)
)

select * from final
```
