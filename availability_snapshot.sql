{% snapshot availability_snapshot %}

{{
    config(
        strategy = 'timestamp',
        unique_key='listing_id',
        updated_at = 'SCRAPED_DATE',
        alias='availability'
    )
}}

SELECT LISTING_ID,
       SCRAPED_DATE::TIMESTAMP,
       HAS_AVAILABILITY,
       AVAILABILITY_30
FROM {{ref('b_listings')}}

{%endsnapshot%}