{{
    config(
        unique_key='lga_code_2016',
        alias='nsw_lga_2016census_g02'
    )
}}

select * from {{ source('raw', 'raw_2016census_g02_nsw_lga') }}