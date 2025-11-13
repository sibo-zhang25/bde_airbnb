{% snapshot housing_info_snapshot %}

{{
    config(
        strategy = 'timestamp',
        unique_key='listing_id',
        updated_at = 'SCRAPED_DATE',
        alias='housing_info'
    )
}}

SELECT LISTING_ID,
       SCRAPED_DATE::TIMESTAMP,
       LISTING_NEIGHBOURHOOD,
       PROPERTY_TYPE,
       ROOM_TYPE,
       ACCOMMODATES
FROM {{ref('b_listings')}}

{%endsnapshot%}