{%- materialization multi_stage_table, default %}
    {%- set identifier = model['alias'] -%}
    {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
    {%- set backup_identifier = model['name'] + '__dbt_backup' -%}

    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(identifier=identifier,
                                                    schema=schema,
                                                    database=database,
                                                    type='table') -%}
    {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                        schema=schema,
                                                        database=database,
                                                        type='table') -%}


    {%- set backup_relation_type = 'table' if old_relation is none else old_relation.type -%}
    {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                    schema=schema,
                                                    database=database,
                                                    type=backup_relation_type) -%}

    {%- set stages = config.get('stages') %}
    {%- if stages is not sequence %}
        {{ exceptions.raise_compiler_error("Invalid `stages`. must be sequence. Got:" ~ number) }}
    {%- endif %}
    {%- if (stages | length) == 0 %}
        {{ exceptions.raise_compiler_error("Invalid `stages`. stage must be greater than or equal to 1.") }}
    {%- endif %}

    {%- set stage_relations = {} %}
    {%- set stage_sqls = {} %}
    {%- for stage in stages %}
        {%- set stage_unrenderd_sql = "{%- set current_stage = '"~stage~"' %}\n"~ model['raw_code'] %}
        {%- set stage_renderd_sql = render(stage_unrenderd_sql) | trim %}
        {%- if (stage_renderd_sql | length) == 0 %}
            {{ exceptions.raise_compiler_error("state `"~stage~"` sql is empty") }}
        {%- endif %}
        {%- do stage_sqls.update({stage: stage_renderd_sql}) %}

        {%- set stage_relation = make_temp_relation(target_relation) -%}
        {%- do stage_relations.update({stage: stage_relation}) %}
    {%- endfor %}

    -- drop the temp relations if they exists for some reason
    {{ adapter.drop_relation(intermediate_relation) }}
    {{ adapter.drop_relation(backup_relation) }}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {%- for stage in stages %}
        {%- do log("START sql "~target_relation~" stage "~stage, info=True) %}
        {% call statement(stage) -%}
            {{ create_table_as(True, stage_relations[stage], stage_sqls[stage]) }}
        {%- endcall %}
        {%- do log("OK sql "~target_relation~" stage "~stage, info=True) %}
    {%- endfor %}

    -- build model
    {%- set build_sql %}
        {%- for stage in stages %}
        select * from {{ stage_relations[stage] }}
        {%- if not loop.last %} union all {%- endif %}
        {%- endfor %}
    {%- endset %}
    {% call statement('main') -%}
        {{ create_table_as(False, intermediate_relation, build_sql) }}
    {%- endcall %}

    -- cleanup
    {% if old_relation is not none %}
        {{ adapter.rename_relation(target_relation, backup_relation) }}
    {% endif %}

    {{ adapter.rename_relation(intermediate_relation, target_relation) }}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    -- finally, drop the existing/backup relation after the commit
    {{ drop_relation_if_exists(backup_relation) }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}
