{{
    config(
        unique_key='transaction_id',
        alias='g_fact'
    )
}}

SELECT transaction_id,
       listing_id,
       CASE WHEN host_id IN (SELECT DISTINCT host_id FROM {{ref('g_dim_host_info')}}) THEN host_id ELSE 0 END AS host_id,
       scraped_date AS date, 
       price
FROM {{ref('s_fact')}}