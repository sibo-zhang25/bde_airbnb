{{
        config(
            unique_key="listing_id",
            alias="s_host_info",
        )
}}


select
        host_id,
        listing_id,
        scraped_date::timestamp,
        host_is_superhost,
        host_name,
        host_neighbourhood,
        {# convert to standard DATE type#}
        case
            when host_since = 'NaN' then null::date else to_date(host_since, 'D/M/YY')
        end as host_since

from {{ ref('b_listings') }}