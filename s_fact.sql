{{
    config(
        unique_key='transaction_id',
        alias='s_fact'
    )
}}
{#third normal form#}
SELECT {{ dbt_utils.generate_surrogate_key(['listing_id', 'host_id','price']) }} as transaction_id,
       listing_id,
       host_id,
       scraped_date,
       price
FROM {{ref('b_listings')}}