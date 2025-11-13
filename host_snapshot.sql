{% snapshot host_snapshot %}

    {{
        config(
            strategy="timestamp",
            unique_key="listing_id",
            updated_at="SCRAPED_DATE",
            alias="info_host"
        )
    }}

    select *
    from {{ ref('s_host_info') }}

{% endsnapshot %}