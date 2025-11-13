{{
    config(
        unique_key='listing_id',
        alias='g_dim_availability'
    )
}}
{#normalised but not 2NF#}
SELECT  listing_id,
        scraped_date::DATE,
        HAS_AVAILABILITY,
        AVAILABILITY_30,
        dbt_valid_from AS valid_from,
        dbt_valid_to AS valid_to
FROM {{ref('availability_snapshot')}} 