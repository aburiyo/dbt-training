with
    source_customers as (select * from {{ source("jaffle_shop", "customers") }}),

    source_orders as (select * from {{ source("jaffle_shop", "orders") }}),

    source_payment as (select * from {{ source("stripe", "payment") }}),

    final as (
        select
            c.id,
            first_name,
            last_name,
            count(o.id) as total_orders,
            sum(p.amount) as total_amount
        from source_customers c
        left join source_orders o on c.id = o.user_id
        left join source_payment p on o.id = p.orderid
        group by c.id, first_name, last_name
    )

select *
from final
