{{ config (
    materialized="table"
)}}


select
    id as paymant_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,

    -- divide by 100, convert cents => dollars
    amount / 100 as price,
    created as created_at
from
    {{ source('paying', 'raw_payment') }}
    