{{
    config(
        unique_key='lga_code',
        alias='nsw_census_02'
    )
}}

WITH source AS (

    SELECT REPLACE(lga_code_2016,'LGA','')::INT AS lga_code,
            Median_age_persons,
            Median_mortgage_repay_monthly, 
            Median_tot_prsnl_inc_weekly,
            Median_rent_weekly, 
            Median_tot_fam_inc_weekly,
            Average_num_psns_per_bedroom, 
            Median_tot_hhd_inc_weekly,
            Average_household_size 
    FROM {{ref('b_nsw_lga_2016census_g02')}}
)

SELECT * FROM source