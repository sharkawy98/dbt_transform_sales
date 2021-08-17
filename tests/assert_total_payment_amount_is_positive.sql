select
    order_id,
    sum(price) as total
from
    {{ ref('stg_payments') }}
group by 1
having not(total >= 0)