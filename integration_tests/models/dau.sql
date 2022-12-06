{{
    config(
        materialized='multi_stage_table',
        stages=[
            'webapp',
            'raw',
        ],
    )
}}

select
    '{{ current_stage }}' as stage
    ,{{ dbt.safe_cast(dbt.date_trunc('day', '"timestamp"'), api.Column.translate_type("date")) }} as ymd
    {%- if current_stage == 'webapp' %}
    ,case when platform <> 'Web' then 'App' else 'Web' end as platform
    {%- else %}
    ,platform
    {%- endif %}
    ,count(distinct user_id) as uu
from {{ ref('action_log') }}
group by 1,2,3
