{{
    config(
        alias='dm_property_type'
    )
}}

WITH 
source as(

SELECT EXTRACT(YEAR FROM a.date)::INT AS year,
       EXTRACT(MONTH FROM a.date)::INT AS month,
       a.listing_id,
       a.price,
       d.property_type,
       d.room_type,
       d.ACCOMMODATES,
       c.host_id,
       c.host_is_superhost,
       b.HAS_AVAILABILITY,
       b.AVAILABILITY_30,
       e.REVIEW_SCORES_RATING,
       e.NUMBER_OF_REVIEWS
{#implementing SCD2 via snapshots and timestamp#}
FROM {{ref('g_fact')}} a
LEFT JOIN {{ref('g_dim_availability')}} b ON a.LISTING_ID=b.LISTING_ID
AND a.date::TIMESTAMP >= b.valid_from AND a.date::TIMESTAMP < coalesce(b.valid_to, '9999-12-31'::TIMESTAMP)
LEFT JOIN {{ref('g_dim_host_info')}} c ON a.host_id=c.host_id AND a.listing_id = c.listing_id
AND a.date::TIMESTAMP >= c.valid_from AND a.date::TIMESTAMP < coalesce(c.valid_to, '9999-12-31'::TIMESTAMP)
LEFT JOIN {{ref('g_dim_housing_info')}} d ON a.LISTING_ID=d.LISTING_ID
AND a.date::TIMESTAMP >= d.valid_from AND a.date::TIMESTAMP < coalesce(d.valid_to, '9999-12-31'::TIMESTAMP)
LEFT JOIN {{ref('g_dim_review')}} e ON a.LISTING_ID=e.LISTING_ID
AND a.date::TIMESTAMP >= e.valid_from AND a.date::TIMESTAMP < coalesce(e.valid_to, '9999-12-31'::TIMESTAMP)
)

{# Note:median results are slightly off using 0.5 due to design of PERCENTILE_DISC #}
SELECT property_type,
       room_type,
       accommodates,
       year,
       month,
       100*COUNT(CASE WHEN HAS_AVAILABILITY='t' THEN 1 ELSE NULL END)/COUNT(*) AS active_listings_rate,
       COUNT(DISTINCT host_id) AS distinct_hosts,
       MAX(CASE WHEN HAS_AVAILABILITY='t' THEN price else NULL END) AS max_price_active_listings,
       MIN(CASE WHEN HAS_AVAILABILITY='t' THEN price else NULL END) AS min_price_active_listings,
       ROUND(AVG(CASE WHEN HAS_AVAILABILITY='t' THEN price else NULL END),1) AS avg_price_active_listings,
       PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY price) FILTER(WHERE HAS_AVAILABILITY='t') AS median_price_active_listings,
       CAST(100*COUNT(DISTINCT host_id) FILTER (WHERE host_is_superhost='t')::FLOAT/COUNT(DISTINCT host_id)::FLOAT AS DECIMAL(4,1)) AS superhost_rate,
       ROUND(AVG(CASE WHEN HAS_AVAILABILITY='t' THEN REVIEW_SCORES_RATING else NULL END),1) AS avg_review_score_rating,
       CAST(100*(COUNT(CASE WHEN HAS_AVAILABILITY='t' THEN 1 ELSE NULL END)-LAG(COUNT(CASE WHEN HAS_AVAILABILITY='t' THEN 1 ELSE NULL END)
       ) OVER(PARTITION BY property_type,room_type,accommodates ORDER BY year,month))::FLOAT/LAG(COUNT(CASE WHEN HAS_AVAILABILITY='t' THEN 1 ELSE NULL END)
       ) OVER(PARTITION BY property_type,room_type,accommodates ORDER BY year,month)::FLOAT AS DECIMAL(32,1))AS pct_change_active_listings,

       CAST(100*(COUNT(CASE WHEN HAS_AVAILABILITY='f' THEN 1 ELSE NULL END)-LAG(COUNT(CASE WHEN HAS_AVAILABILITY='f' THEN 1 ELSE NULL END)
       ) OVER(PARTITION BY property_type,room_type,accommodates ORDER BY year,month))::FLOAT/NULLIF(LAG(COUNT(CASE WHEN HAS_AVAILABILITY='f' THEN 1 ELSE NULL END)
       ) OVER(PARTITION BY property_type,room_type,accommodates ORDER BY year,month),0)::FLOAT AS DECIMAL(32,1))AS pct_change_inactive_listings,
       SUM(30-AVAILABILITY_30) FILTER (WHERE HAS_AVAILABILITY='t') AS total_number_of_stays,
       SUM(price*(30-AVAILABILITY_30))/COUNT(*) FILTER(WHERE HAS_AVAILABILITY='t') AS avg_revenue_active_listings
FROM source
GROUP BY month, year, property_type,room_type,accommodates
ORDER BY property_type,room_type,accommodates, year,month