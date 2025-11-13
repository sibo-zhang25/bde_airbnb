{{
    config(
        alias='dm_host_neighbourhood'
    )
}}

WITH 
source as(

SELECT EXTRACT(YEAR FROM a.date)::INT AS year,
       EXTRACT(MONTH FROM a.date)::INT AS month,
       a.listing_id,
       a.price,
       d.listing_neighbourhood,
       e.lga_code AS host_neighbourhood_lga,
       c.host_id,
       b.HAS_AVAILABILITY,
       b.AVAILABILITY_30
{#implementing SCD2 via snapshots and timestamp#}
FROM {{ref('g_fact')}} a
LEFT JOIN {{ref('g_dim_availability')}} b ON a.LISTING_ID=b.LISTING_ID
AND a.date::TIMESTAMP >= b.valid_from AND a.date::TIMESTAMP < coalesce(b.valid_to, '9999-12-31'::TIMESTAMP)
LEFT JOIN {{ref('g_dim_host_info')}} c ON a.host_id=c.host_id AND a.listing_id = c.listing_id
AND a.date::TIMESTAMP >= c.valid_from AND a.date::TIMESTAMP < coalesce(c.valid_to, '9999-12-31'::TIMESTAMP)
LEFT JOIN {{ref('g_dim_housing_info')}} d ON a.LISTING_ID=d.LISTING_ID
AND a.date::TIMESTAMP >= d.valid_from AND a.date::TIMESTAMP < coalesce(d.valid_to, '9999-12-31'::TIMESTAMP)
LEFT JOIN {{ref("b_nsw_lga_code")}} e ON d.listing_neighbourhood=e.lga_name
)

SELECT  host_neighbourhood_lga,
        year,
        month,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        SUM(price*(30-AVAILABILITY_30))/COUNT(*) FILTER(WHERE HAS_AVAILABILITY='t') AS avg_revenue_active_listings,
        SUM(price*(30-AVAILABILITY_30)) FILTER(WHERE HAS_AVAILABILITY='t')/COUNT(DISTINCT host_id) AS revenue_per_host
FROM source
GROUP BY month, year, host_neighbourhood_lga
ORDER BY host_neighbourhood_lga, year,month