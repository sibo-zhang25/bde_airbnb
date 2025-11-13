{{
    config(
        unique_key='listing_id',
        alias='g_dim_host_info'
    )
}}
{#normalised but not 2NF#}
SELECT  host_id,
        listing_id,
        scraped_date::DATE,
        host_is_superhost,
        host_name,
        host_neighbourhood,
        host_since,
        dbt_valid_from AS valid_from,
        dbt_valid_to AS valid_to
FROM {{ref('host_snapshot')}}
UNION ALL
SELECT 0 AS host_id, 
       0 AS listing_id,
       NULL::DATE AS scraped_date,
       NULL::VARCHAR AS host_is_superhost,
       NULL::VARCHAR AS host_name,
       NULL::VARCHAR AS host_neighbourhood,
       NULL::DATE AS host_since,
       NULL::TIMESTAMP AS valid_from,
       NULL::TIMESTAMP AS valid_to