# dbt-materialization-multi-stage-table

The [dbt package](https://docs.getdbt.com/docs/building-a-dbt-project/package-management) for multi-stage table materialization  
**Note: This is a PoC DBT package**.

This DBT package provides a materialization that builds similar SQL in multiple steps with different parameters and then joins them with UNION ALL.
This package has a materialization called `multi_stage_table`.

## Installation

Add to your packages.yml
```yaml
packages:
  - git: "https://github.com/mashiike/dbt-materialization-multi-stage-table"
    revision: v0.0.0
```

## Usage 

dau.sql:
```sql
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
```

This model issues two queries.

stage `webapp`

```sql
  create temporary table dau__dbt_tmp164834455055
  as (
    select
    'webapp' as stage
    ,
    
    cast(date_trunc('day', "timestamp") as date)
 as ymd
    ,case when platform <> 'Web' then 'App' else 'Web' end as platform
    ,count(distinct user_id) as uu
from "postgres".public.action_log
group by 1,2,3
  );
```

stage `raw`

```sql
  create temporary table dau__dbt_tmp164834465812
  as (
    select
    'raw' as stage
    ,
    
    cast(date_trunc('day', "timestamp") as date)
 as ymd
    ,platform
    ,count(distinct user_id) as uu
from "postgres".public.action_log
group by 1,2,3
  );
```

The tables of each stage are then joined and materialized with UNION ALL.

```sql
  create  table "postgres".public.dau__dbt_tmp
  as (
    
        select * from dau__dbt_tmp164834455055 union all
        select * from dau__dbt_tmp164834465812
  );
```

in integration test:

```shell
$ make test-postgres                               
dbt deps
07:49:51  Running with dbt=1.3.1
07:49:52  Installing ../
07:49:52    Installed from <local @ ../>
07:49:52  Installing dbt-labs/dbt_utils
07:49:53    Installed from version 0.9.2
07:49:53    Updated version available: 1.0.0
07:49:53  
07:49:53  Updates available for packages: ['dbt-labs/dbt_utils']                 
Update your versions in packages.yml, then run dbt deps
dbt build --target postgres --full-refresh
07:50:06  Running with dbt=1.3.1
07:50:07  Found 1 model, 1 test, 0 snapshots, 0 analyses, 487 macros, 0 operations, 2 seed files, 0 sources, 0 exposures, 0 metrics
07:50:07  
07:50:07  Concurrency: 8 threads (target='postgres')
07:50:07  
07:50:07  1 of 4 START seed file public.action_log ....................................... [RUN]
07:50:07  2 of 4 START seed file public.expected ......................................... [RUN]
07:50:07  2 of 4 OK loaded seed file public.expected ..................................... [CREATE 5 in 0.29s]
07:50:07  1 of 4 OK loaded seed file public.action_log ................................... [CREATE 11 in 0.30s]
07:50:07  3 of 4 START sql multi_stage_table model public.dau ............................ [RUN]
07:50:08  START sql "postgres".public.dau stage webapp
07:50:08  OK sql "postgres".public.dau stage webapp
07:50:08  START sql "postgres".public.dau stage raw
07:50:08  OK sql "postgres".public.dau stage raw
07:50:08  3 of 4 OK created sql multi_stage_table model public.dau ....................... [SELECT 5 in 0.27s]
07:50:08  4 of 4 START test dbt_utils_equality_dau_stage__ymd__platform__uu__ref_expected_  [RUN]
07:50:08  4 of 4 PASS dbt_utils_equality_dau_stage__ymd__platform__uu__ref_expected_ ..... [PASS in 0.07s]
07:50:08  
07:50:08  Finished running 2 seeds, 1 multi_stage_table model, 1 test in 0 hours 0 minutes and 0.88 seconds (0.88s).
07:50:08  
07:50:08  Completed successfully
07:50:08  
07:50:08  Done. PASS=4 WARN=0 ERROR=0 SKIP=0 TOTAL=4
```

## LICENSE

MIT 

However, some of the code has been modified from https://github.com/dbt-labs/dbt-core, the original license of which is [here](https://github.com/dbt-labs/dbt-core/blob/v1.0.2/License.md)
