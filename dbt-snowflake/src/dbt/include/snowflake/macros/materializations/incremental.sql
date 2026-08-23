{% macro dbt_snowflake_get_tmp_relation_type(strategy, unique_key, language) %}
  {# Compatibility entry point: projects may still override this macro. #}
  {% set catalog_relation = adapter.build_catalog_relation(config.model) %}
  {% set plan = adapter.plan_incremental_mutation(
      strategy,
      language=language,
      unique_key=unique_key,
      requested_temp_relation_type=config.get('tmp_relation_type'),
      catalog_relation=catalog_relation
  ) %}
  {% do adapter.get_incremental_plan_macro(context, plan) %}
  {{ return(plan.temp_relation_type.value) }}
{% endmacro %}


{% macro resolve_incremental_tmp_relation(tmp_relation) %}
  {{ return(adapter.dispatch('resolve_incremental_tmp_relation', 'dbt')(tmp_relation)) }}
{% endmacro %}


{% macro snowflake__resolve_incremental_tmp_relation(tmp_relation) %}
  {#--
    Override this macro in your project to control where the incremental
    tmp relation is created. Useful for redirecting to a dedicated scratch
    schema to avoid name collisions when multiple runs share the same
    target schema.

    Example:
      {% macro snowflake__resolve_incremental_tmp_relation(tmp_relation) %}
        {{ return(tmp_relation.incorporate(schema='scratch')) }}
      {% endmacro %}
  --#}
  {{ return(tmp_relation) }}
{% endmacro %}

{% materialization incremental, adapter='snowflake', supported_languages=['sql', 'python'] -%}

  {% set original_query_tag = set_query_tag() %}

  {#-- Set vars --#}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set language = model['language'] -%}

  {%- set identifier = this.name -%}
  {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

  {%- set is_catalog_linked_db = snowflake__is_catalog_linked_database(relation=none, catalog_relation=catalog_relation) -%}

  {%- set target_relation = api.Relation.create(
	identifier=identifier,
	schema=schema,
	database=database,
	type='table',
	table_format=catalog_relation.table_format,
  ) -%}

  {% set existing_relation = load_relation(this) %}

  {#-- The temp relation will be a view (faster) or temp table, depending on upsert/merge strategy --#}
  {%- set unique_key = config.get('unique_key') -%}
  {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
  {% set incremental_plan = adapter.plan_incremental_mutation(
      incremental_strategy,
      language=language,
      unique_key=unique_key,
      requested_temp_relation_type=config.get('tmp_relation_type'),
      catalog_relation=catalog_relation
  ) %}
  {% set strategy_sql_macro_func = adapter.get_incremental_plan_macro(context, incremental_plan) %}
  {% set tmp_relation_type = dbt_snowflake_get_tmp_relation_type(incremental_strategy, unique_key, language) %}

  {% if is_catalog_linked_db %}
    {% set tmp_relation = make_temp_relation(this).incorporate(type=tmp_relation_type, catalog=catalog_relation.catalog_name, is_table=true) %}
  {% else %}
    {#-- Transient tables are dropped with DROP TABLE, so the relation type must be 'table' --#}
    {% set tmp_relation_object_type = 'table' if tmp_relation_type == 'transient' else tmp_relation_type %}
    {% set tmp_relation = make_temp_relation(this).incorporate(type=tmp_relation_object_type) %}
  {% endif %}
  {% set tmp_relation = resolve_incremental_tmp_relation(tmp_relation) %}

  {% set grant_config = config.get('grants') %}

  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}

  {% elif existing_relation.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}

  {% elif full_refresh_mode %}
    {% if target_relation.needs_to_drop(existing_relation) %}
      {{ drop_relation_if_exists(existing_relation) }}
    {% endif %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}

  {% elif target_relation.table_format != existing_relation.table_format %}
    {% do exceptions.raise_compiler_error(
        "Unable to update the incremental model `" ~ target_relation.identifier ~ "` from `" ~ existing_relation.table_format ~ "` to `" ~ target_relation.table_format ~ "` due to Snowflake limitation. Please execute with --full-refresh to drop the table and recreate in the new catalog.'"
      )
    %}

  {% else %}
    {#-- Create the temp relation as a view, temp table, or transient table --#}
    {% if is_catalog_linked_db %}
        {%- call statement('create_tmp_relation', language=language) -%}
          {{ create_table_as(False, tmp_relation, compiled_code, language) }}
        {%- endcall -%}
    {% elif tmp_relation_type == 'view' %}
        {%- call statement('create_tmp_relation') -%}
          {{ snowflake__create_view_as_with_temp_flag(tmp_relation, compiled_code, True) }}
        {%- endcall -%}
    {% elif tmp_relation_type == 'transient' %}
        {%- call statement('create_tmp_relation', language=language) -%}
          {{ snowflake__create_table_transient_sql(tmp_relation, compiled_code) }}
        {%- endcall -%}
    {% else %}
        {%- call statement('create_tmp_relation', language=language) -%}
          {{ create_table_as(True, tmp_relation, compiled_code, language) }}
        {%- endcall -%}
    {% endif %}

    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#-- Build typed late-bound arguments for the resolved strategy renderer --#}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% set strategy_arguments = adapter.plan_incremental_arguments(
        target_relation=target_relation,
        temp_relation=tmp_relation,
        unique_key=unique_key,
        dest_columns=dest_columns,
        incremental_predicates=incremental_predicates,
        adapter_arguments={'catalog_relation': catalog_relation}
    ) %}

    {%- call statement('main') -%}
      {{ strategy_sql_macro_func(strategy_arguments.to_macro_dict()) }}
    {%- endcall -%}
  {% endif %}


  {% do drop_relation_if_exists(tmp_relation) %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = target_relation.incorporate(type='table') %}

  {% set should_revoke =
   should_revoke(existing_relation.is_table, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}


{% macro snowflake__get_incremental_default_sql(arg_dict) %}
  {{ return(get_incremental_merge_sql(arg_dict)) }}
{% endmacro %}
