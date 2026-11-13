{#
  Custom materialization: creates a Snowflake TRANSIENT table.
  Used for silver/enrichment models where we don't need Fail-safe /
  long Time Travel retention - saves storage cost on data that can be
  rebuilt from bronze at any time.
#}

{% materialization transient_table, adapter='snowflake' %}

  {%- set target_relation = api.Relation.create(
        identifier=this.identifier, schema=this.schema, database=this.database, type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    create or replace transient table {{ target_relation }}
    {{ "copy grants" if config.get('copy_grants', false) }}
    as (
      {{ sql }}
    )
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
