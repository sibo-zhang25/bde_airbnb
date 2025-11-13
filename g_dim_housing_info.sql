{{
    config(
        unique_key='listing_id',
        alias='g_dim_housing_info'
    )
}}
{#normalised but not 2NF#}
SELECT  listing_id,
        scraped_date::DATE,
        LISTING_NEIGHBOURHOOD,
        PROPERTY_TYPE,
        ROOM_TYPE,
        ACCOMMODATES,
        dbt_valid_from AS valid_from,
        dbt_valid_to AS valid_to
FROM {{ref('housing_info_snapshot')}} 