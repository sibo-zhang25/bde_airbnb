{{
    config(
        unique_key='listing_id',
        alias='g_dim_review'
    )
}}
{#normalised but not 2NF#}
SELECT  listing_id,
        scraped_date::DATE,
        number_of_reviews,
        NULLIF(REVIEW_SCORES_RATING,'NaN') AS REVIEW_SCORES_RATING, 
        NULLIF(REVIEW_SCORES_ACCURACY,'NaN') AS REVIEW_SCORES_ACCURACY,
        NULLIF(REVIEW_SCORES_CLEANLINESS,'NaN') AS REVIEW_SCORES_CLEANLINESS,
        NULLIF(REVIEW_SCORES_CHECKIN,'NaN') AS REVIEW_SCORES_CHECKIN,
        NULLIF(REVIEW_SCORES_COMMUNICATION,'NaN') AS REVIEW_SCORES_COMMUNICATION,
        NULLIF(REVIEW_SCORES_VALUE,'NaN') AS REVIEW_SCORES_VALUE,
        dbt_valid_from AS valid_from,
        dbt_valid_to AS valid_to
FROM {{ref('review_snapshot')}} 