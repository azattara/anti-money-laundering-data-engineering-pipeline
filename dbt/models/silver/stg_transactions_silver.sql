{{ config(materialized='table', schema='aml_silver') }}

select
    *,
    row_number() over (
        partition by 
            from_bank, 
            account2, 
            to_bank, 
            account4, 
            cast(amount_paid as string), 
            timestamp 
        order by _ingested_at desc
    ) as row_num
from {{ source('aml_silver', 'transactions') }}
where amount_paid > 0


