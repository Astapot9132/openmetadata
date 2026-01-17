from typing import Optional

from cfg import CLICKHOUSE_DATABASE_SCHEMA, CLICKHOUSE_TABLE_INCLUDES, CLICKHOUSE_TABLE_EXCLUDES, \
    CLICKHOUSE_SCHEMA_EXCLUDES, CLICKHOUSE_SCHEMA_INCLUDES, CLICKHOUSE_DB_INCLUDES, CLICKHOUSE_DB_EXCLUDES


def build_filter_pattern(
    includes: list[str], excludes: list[str]
) -> Optional[dict[str, list[str]]]:
    pattern = {}
    if includes:
        pattern["includes"] = includes
    if excludes:
        pattern["excludes"] = excludes
    return pattern or None

def create_filters() -> dict[str, dict[str, list[str]]]:
    filters = {}

    database_pattern = build_filter_pattern(CLICKHOUSE_DB_INCLUDES, CLICKHOUSE_DB_EXCLUDES)
    if database_pattern:
        filters["databaseFilterPattern"] = database_pattern

    schema_pattern = build_filter_pattern(CLICKHOUSE_SCHEMA_INCLUDES, CLICKHOUSE_SCHEMA_EXCLUDES)
    if schema_pattern:
        filters["schemaFilterPattern"] = schema_pattern

    table_pattern = build_filter_pattern(CLICKHOUSE_TABLE_INCLUDES, CLICKHOUSE_TABLE_EXCLUDES)
    if table_pattern:
        filters["tableFilterPattern"] = table_pattern
        
    return filters