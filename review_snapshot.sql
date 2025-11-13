{% snapshot review_snapshot %}

{{
    config(
        strategy = 'timestamp',
        unique_key='listing_id',
        updated_at = 'SCRAPED_DATE',
        alias='review'
    )
}}

SELECT LISTING_ID,
SCRAPED_DATE::TIMESTAMP,
NUMBER_OF_REVIEWS,
review_scores_rating,
review_scores_accuracy,
review_scores_cleanliness,
review_scores_checkin,
review_scores_communication,
review_scores_value
FROM {{ref('b_listings')}}

{%endsnapshot%}