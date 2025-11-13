{{
    config(
        unique_key='lga_name',
        alias='nsw_lga_code'
    )
}}

SELECT * FROM {{ source('raw', 'raw_nsw_lga_code') }}