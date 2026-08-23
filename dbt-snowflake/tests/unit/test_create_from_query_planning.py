from types import SimpleNamespace

import pytest

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.planning import (
    CatalogBindingState,
    CatalogFacts,
    CreateFromQueryFacts,
    CreateFromQueryStrategy,
    DdlAtomicity,
    FormatFacts,
    IncrementalCatalogStaging,
    IncrementalMutationFacts,
    IncrementalMutationStrategy,
    IncrementalSourceConsistency,
    IncrementalTempRelationType,
    IncrementalUniqueKeyRequirement,
    RelationFacts,
    RuntimeFacts,
    resolve_create_from_query_offers,
    resolve_incremental_mutation_offers,
)
from dbt.adapters.snowflake import SnowflakeAdapter


def _facts(catalog_type, *, provider=None, table_format=None):
    return CreateFromQueryFacts(
        relation=RelationFacts(
            database="analytics",
            schema="mart",
            identifier="orders",
            relation_type="table",
        ),
        catalog=CatalogFacts(
            state=CatalogBindingState.RESOLVED,
            integration_name="analytics_catalog",
            catalog_type=catalog_type,
            catalog_name="platform_catalog",
            catalog_provider=provider,
        ),
        format=FormatFacts(table_format=table_format),
        runtime=RuntimeFacts(engine="snowflake"),
    )


@pytest.mark.parametrize(
    "catalog_type,table_format,renderer",
    [
        ("info_schema", "default", "snowflake__render_create_from_query_info_schema"),
        ("built_in", "iceberg", "snowflake__render_create_from_query_built_in"),
        (
            "iceberg_rest",
            "iceberg",
            "snowflake__render_create_from_query_iceberg_rest",
        ),
    ],
)
def test_snowflake_catalog_facts_select_ctas_renderer(catalog_type, table_format, renderer):
    facts = _facts(catalog_type, table_format=table_format)

    offers = SnowflakeAdapter.get_create_from_query_strategy_offers(None, False, facts)
    plan = resolve_create_from_query_offers(
        temporary=False,
        facts=facts,
        offers=offers,
    )

    assert plan.strategy == CreateFromQueryStrategy.CTAS
    assert plan.renderer_macro == renderer
    assert plan.atomicity == DdlAtomicity.BEST_EFFORT


def test_snowflake_glue_facts_select_create_then_insert():
    facts = _facts("iceberg_rest", provider="glue", table_format="iceberg")

    offers = SnowflakeAdapter.get_create_from_query_strategy_offers(None, False, facts)
    plan = resolve_create_from_query_offers(
        temporary=False,
        facts=facts,
        offers=offers,
    )

    assert offers[0].reason == "Snowflake Glue catalog-linked databases do not support CTAS"
    assert plan.strategy == CreateFromQueryStrategy.CREATE_THEN_INSERT
    assert plan.renderer_macro == "snowflake__render_create_from_query_glue"
    assert plan.atomicity == DdlAtomicity.BEST_EFFORT


def test_snowflake_temporary_operation_selects_temporary_ctas():
    facts = _facts("built_in", table_format="iceberg")

    offers = SnowflakeAdapter.get_create_from_query_strategy_offers(None, True, facts)
    plan = resolve_create_from_query_offers(
        temporary=True,
        facts=facts,
        offers=offers,
    )

    assert plan.strategy == CreateFromQueryStrategy.CTAS
    assert plan.renderer_macro == "snowflake__render_create_from_query_temporary"
    assert plan.atomicity == DdlAtomicity.STATEMENT


def test_snowflake_unknown_catalog_is_explicitly_unsupported():
    facts = _facts("future_catalog", table_format="future_format")

    offers = SnowflakeAdapter.get_create_from_query_strategy_offers(None, False, facts)
    plan = resolve_create_from_query_offers(
        temporary=False,
        facts=facts,
        offers=offers,
    )

    assert plan.strategy == CreateFromQueryStrategy.UNSUPPORTED
    assert "future_catalog" in plan.reason


def test_snowflake_catalog_provider_is_resolved_in_python():
    adapter = SimpleNamespace(
        _create_from_query_fact_value=BaseAdapter._create_from_query_fact_value,
    )
    catalog_relation = SimpleNamespace(catalog_linked_database_type="GLUE")

    provider = SnowflakeAdapter.get_create_from_query_catalog_provider(
        adapter, catalog_relation, model=None
    )

    assert provider == "glue"


def _incremental_plan(
    strategy,
    *,
    language="sql",
    unique_key_present=False,
    requested_temp_relation_type=None,
    catalog_staging=IncrementalCatalogStaging.STANDARD,
):
    adapter = SimpleNamespace(
        builtin_incremental_strategies=lambda: [
            "append",
            "delete+insert",
            "merge",
            "insert_overwrite",
            "microbatch",
        ],
        valid_incremental_strategies=lambda: [
            "append",
            "delete+insert",
            "merge",
            "insert_overwrite",
            "microbatch",
        ],
    )
    facts = IncrementalMutationFacts(
        requested_strategy=strategy,
        language=language,
        unique_key_present=unique_key_present,
        requested_temp_relation_type=requested_temp_relation_type,
        catalog_staging=catalog_staging,
    )
    offers = SnowflakeAdapter.get_incremental_mutation_strategy_offers(adapter, facts)
    return resolve_incremental_mutation_offers(facts=facts, offers=offers)


@pytest.mark.parametrize("strategy", ["default", "merge", "append", "insert_overwrite"])
def test_snowflake_single_evaluation_strategies_default_to_view(strategy):
    plan = _incremental_plan(strategy)

    assert plan.temp_relation_type == IncrementalTempRelationType.VIEW
    assert plan.requirements.source_consistency == IncrementalSourceConsistency.SINGLE_EVALUATION


@pytest.mark.parametrize("strategy", ["delete+insert", "microbatch"])
def test_snowflake_keyed_multi_statement_strategies_require_stable_staging(strategy):
    plan = _incremental_plan(strategy, unique_key_present=True)

    assert plan.temp_relation_type == IncrementalTempRelationType.TABLE
    assert plan.requirements.source_consistency == IncrementalSourceConsistency.STABLE_REUSE

    rejected = _incremental_plan(
        strategy,
        unique_key_present=True,
        requested_temp_relation_type="view",
    )
    assert rejected.strategy == IncrementalMutationStrategy.UNSUPPORTED
    assert "[table, transient]" in rejected.reason


def test_snowflake_append_and_insert_overwrite_ignore_unique_key():
    for strategy in ("append", "insert_overwrite"):
        plan = _incremental_plan(strategy, unique_key_present=True)
        assert plan.requirements.unique_key == IncrementalUniqueKeyRequirement.IGNORED


@pytest.mark.parametrize(
    "facts",
    [
        {"language": "python"},
        {"catalog_staging": IncrementalCatalogStaging.PERMANENT_TABLE_ONLY},
    ],
)
def test_snowflake_runtime_and_catalog_facts_require_table_staging(facts):
    plan = _incremental_plan("merge", **facts)

    assert plan.temp_relation_type == IncrementalTempRelationType.TABLE
    assert plan.requirements.allowed_temp_relation_types == (IncrementalTempRelationType.TABLE,)
    if "catalog_staging" in facts:
        assert plan.catalog_staging == IncrementalCatalogStaging.PERMANENT_TABLE_ONLY


def test_snowflake_custom_strategy_preserves_table_staging_default():
    plan = _incremental_plan("my_strategy")

    assert plan.strategy == IncrementalMutationStrategy.CUSTOM
    assert plan.temp_relation_type == IncrementalTempRelationType.TABLE
