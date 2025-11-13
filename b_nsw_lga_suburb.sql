{{
    config(
        unique_key='suburb_name',
        alias='nsw_lga_suburb'
    )
}}

SELECT * FROM {{ source('raw', 'raw_nsw_lga_suburb') }}