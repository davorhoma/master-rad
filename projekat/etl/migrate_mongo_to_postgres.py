"""Copy MongoDB collections to PostgreSQL using per-table staging swaps.

WARNING: Do not run this after applications begin writing directly to PostgreSQL.
Each completed collection is replaced by the source MongoDB collection contents.
"""

from __future__ import annotations

import json
import hashlib
import math
import os
import re
import uuid
from collections.abc import Mapping
from datetime import date, datetime, timezone
from decimal import Decimal
from io import StringIO
from typing import Any

import pandas as pd
from bson import Decimal128, ObjectId
from pymongo import MongoClient
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    create_engine,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.engine import URL
from sqlalchemy.schema import CreateSchema

SAMPLE_SIZE = 1000
CHUNK_SIZE = 10_000
SYSTEM_DATABASES = {"admin", "config", "local"}
INTEGER_MIN = -(2**63)
INTEGER_MAX = 2**63 - 1


def normalize_identifier(value: str) -> str:
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_").lower()
    value = re.sub(r"_+", "_", value) or "unnamed"
    if value[0].isdigit():
        value = f"collection_{value}"
    return value[:63]


def _is_simple_document(value: Mapping[str, Any], depth: int = 0) -> bool:
    if depth >= 3:
        return False
    return all(
        not isinstance(child, (list, tuple, set))
        and (not isinstance(child, Mapping) or _is_simple_document(child, depth + 1))
        for child in value.values()
    )


def _flatten_document(document: Mapping[str, Any]) -> dict[str, Any]:
    flattened: dict[str, Any] = {}

    def visit(value: Any, path: tuple[str, ...]) -> None:
        name = normalize_identifier("_".join(path))
        if isinstance(value, Mapping) and value and _is_simple_document(value):
            for child_name, child_value in value.items():
                visit(child_value, (*path, str(child_name)))
            return
        if isinstance(value, Mapping) and not value:
            flattened[name] = {}
            return
        flattened[name] = value

    for key, value in document.items():
        visit(value, (str(key),))
    return flattened


def _source_column_mapping(document: Mapping[str, Any]) -> dict[str, str]:
    mapping: dict[str, str] = {}

    def visit(value: Any, path: tuple[str, ...]) -> None:
        if isinstance(value, Mapping) and value and _is_simple_document(value):
            for child_name, child_value in value.items():
                visit(child_value, (*path, str(child_name)))
            return
        source_name = ".".join(path)
        mapping[source_name] = normalize_identifier("_".join(path))

    for key, value in document.items():
        visit(value, (str(key),))
    return mapping


def _unwrap(value: Any) -> Any:
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str) and re.match(r"^\d{4}-\d{2}-\d{2}(?:[T ]|$)", value):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            try:
                return date.fromisoformat(value)
            except ValueError:
                pass
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Decimal128):
        return value.to_decimal()
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def _sample_type(values: list[Any]):
    present = [_unwrap(value) for value in values if value is not None]
    if not present:
        return Text()

    if all(isinstance(value, bool) for value in present):
        return Boolean()
    if all(isinstance(value, int) and not isinstance(value, bool) for value in present):
        smallest = min(int(value) for value in present)
        largest = max(int(value) for value in present)
        return BigInteger() if INTEGER_MIN <= smallest and largest <= INTEGER_MAX else Numeric(38, 0)
    if all(isinstance(value, (int, float, Decimal)) and not isinstance(value, bool) for value in present):
        if any(isinstance(value, Decimal) for value in present):
            return Numeric(38, 12)
        return Float()
    if all(isinstance(value, datetime) for value in present):
        return DateTime(timezone=True)
    if all(isinstance(value, (datetime, date)) for value in present):
        return DateTime(timezone=True)
    if all(type(value) is date for value in present):
        return Date()
    if any(isinstance(value, (Mapping, list, tuple, set)) for value in present):
        return JSONB()
    return Text()


def _json_safe(value: Any) -> Any:
    value = _unwrap(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(child) for key, child in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(child) for child in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _coerce(value: Any, column_type: Any) -> Any:
    value = _unwrap(value)
    if value is None:
        return None
    if isinstance(column_type, JSONB):
        return _json_safe(value)
    if isinstance(column_type, DateTime):
        if isinstance(value, date) and not isinstance(value, datetime):
            value = datetime.combine(value, datetime.min.time())
        if isinstance(value, datetime) and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(column_type, Date):
        return value.date() if isinstance(value, datetime) else value
    if isinstance(column_type, Boolean):
        return bool(value)
    if isinstance(column_type, (BigInteger, Numeric, Float)):
        return value
    if isinstance(column_type, (String, Text)):
        if isinstance(value, (Mapping, list, tuple, set)):
            return json.dumps(_json_safe(value), ensure_ascii=False)
        return str(value)
    return value


def _promote_mixed_columns(
    connection: Any,
    engine: Engine,
    table: Table,
    documents: list[dict[str, Any]],
) -> None:
    for column in table.columns:
        values = [
            _unwrap(document[column.name])
            for document in documents
            if column.name in document and document[column.name] is not None
        ]
        if not values or isinstance(column.type, (JSONB, Text, String)):
            continue

        if isinstance(column.type, (BigInteger, Numeric, Float)):
            compatible = all(isinstance(value, (int, float, Decimal)) and not isinstance(value, bool) for value in values)
        elif isinstance(column.type, Boolean):
            compatible = all(isinstance(value, bool) for value in values)
        elif isinstance(column.type, DateTime):
            compatible = all(isinstance(value, (date, datetime)) for value in values)
        elif isinstance(column.type, Date):
            compatible = all(isinstance(value, date) for value in values)
        else:
            compatible = True

        if not compatible:
            connection.execute(
                text(
                    f"ALTER TABLE {_quote(engine, table.schema)}.{_quote(engine, table.name)} "
                    f"ALTER COLUMN {_quote(engine, column.name)} TYPE TEXT USING "
                    f"{_quote(engine, column.name)}::text"
                )
            )
            column.type = Text()
            print(f"  Mixed types promoted to text: {column.name}")


def _source_schema(database_name: str) -> str:
    if database_name == "historical_data":
        return normalize_identifier(os.getenv("MONGO_DEFAULT_SCHEMA", "public"))
    return normalize_identifier(database_name)


def _quote(engine: Engine, identifier: str) -> str:
    return engine.dialect.identifier_preparer.quote(identifier)


def _add_unseen_columns(
    connection: Any,
    engine: Engine,
    table: Table,
    documents: list[dict[str, Any]],
    mapping: dict[str, str],
) -> None:
    known = set(table.c.keys())
    for document in documents:
        for normalized_name in document:
            if normalized_name not in known:
                mapping[f"<observed after sample>:{normalized_name}"] = normalized_name
                connection.execute(
                    text(
                        f"ALTER TABLE {_quote(engine, table.schema)}."
                        f"{_quote(engine, table.name)} ADD COLUMN "
                        f"{_quote(engine, normalized_name)} JSONB"
                    )
                )
                table.append_column(Column(normalized_name, JSONB()))
                known.add(normalized_name)
                print(f"  New field mapped to JSONB: {normalized_name}")


def _create_indexes(
    connection: Any,
    engine: Engine,
    source_collection: Any,
    table: Table,
    token: str,
) -> None:
    schema_name = _quote(engine, table.schema)
    table_name = _quote(engine, table.name)
    for index in source_collection.list_indexes():
        if index["name"] == "_id_" and any(
            column.primary_key and column.info.get("mongo_id") for column in table.columns
        ):
            continue
        key_parts = []
        for source_field, direction in index["key"].items():
            column_name = normalize_identifier(source_field.replace(".", "_"))
            if column_name not in table.c:
                print(f"  Skipping index {index['name']}: field {source_field} unavailable")
                key_parts = []
                break
            suffix = " DESC" if direction == -1 else " ASC"
            quoted_column = _quote(engine, column_name)
            if index.get("unique", False) and isinstance(table.c[column_name].type, (Text, String)):
                key_parts.append(f"md5({quoted_column}){suffix}")
            else:
                key_parts.append(f"{quoted_column}{suffix}")
        if not key_parts:
            continue

        index_digest = hashlib.sha1(
            f"{table.schema}.{table.name}.{index['name']}.{token}".encode("utf-8")
        ).hexdigest()[:12]
        index_name = f"idx_{index_digest}"
        unique = "UNIQUE " if index.get("unique", False) else ""
        connection.execute(
            text(
                f"CREATE {unique}INDEX {_quote(engine, index_name)} ON "
                f"{schema_name}.{table_name} ({', '.join(key_parts)})"
            )
        )


def _collection_columns(
    sample: list[dict[str, Any]],
) -> tuple[list[Column[Any]], dict[str, str]]:
    flattened = [_flatten_document(document) for document in sample]
    source_to_normalized: dict[str, str] = {}
    for document in sample:
        for source_name, flattened_name in _source_column_mapping(document).items():
            previous = source_to_normalized.get(source_name)
            if previous is not None and previous != flattened_name:
                raise ValueError(f"Column mapping collision for {source_name!r}")
            source_to_normalized[source_name] = flattened_name

    all_names = sorted({name for document in flattened for name in document})
    if not all_names:
        all_names = ["_id"]
    columns = []
    mongo_id_column = source_to_normalized.get("_id")
    for name in all_names:
        column_type = _sample_type([document.get(name) for document in flattened])
        columns.append(
            Column(
                name,
                column_type,
                primary_key=True,
                server_default=text("gen_random_uuid()::text"),
                info={"mongo_id": True},
            )
            if name == mongo_id_column
            else Column(name, column_type, nullable=True)
        )
    return columns, source_to_normalized


def _migrate_collection(
    engine: Engine,
    source_database: Any,
    database_name: str,
    collection_name: str,
) -> tuple[int, int]:
    source_collection = source_database[collection_name]
    schema_name = _source_schema(database_name)
    table_name = normalize_identifier(collection_name)
    sample_documents = list(source_collection.find({}, limit=SAMPLE_SIZE))
    sample_columns, mapping = _collection_columns(sample_documents)
    token = uuid.uuid4().hex[:10]
    staging_name = f"{table_name[:42]}__stage_{token}"
    metadata = MetaData()

    with engine.begin() as connection:
        connection.execute(CreateSchema(schema_name, if_not_exists=True))
        staging_table = Table(
            staging_name,
            metadata,
            *sample_columns,
            schema=schema_name,
        )
        metadata.create_all(connection, tables=[staging_table])
        expected_rows = source_collection.count_documents({})
        print(
            f"Migrating {database_name}.{collection_name} -> "
            f"{schema_name}.{table_name}; sample={len(sample_documents)}, "
            f"rows={expected_rows}"
        )
        print("  Column mapping: " + json.dumps(mapping, ensure_ascii=False, sort_keys=True))

        cursor = source_collection.find({}, no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        loaded_rows = 0
        pending: list[dict[str, Any]] = []
        try:
            for source_document in cursor:
                pending.append(_flatten_document(source_document))
                if len(pending) >= CHUNK_SIZE:
                    _add_unseen_columns(connection, engine, staging_table, pending, mapping)
                    _promote_mixed_columns(connection, engine, staging_table, pending)
                    _insert_batch(connection, staging_table, pending)
                    loaded_rows += len(pending)
                    pending.clear()
                    if loaded_rows % 100_000 == 0:
                        print(f"  Loaded {loaded_rows:,}/{expected_rows:,}")
            if pending:
                _add_unseen_columns(connection, engine, staging_table, pending, mapping)
                _promote_mixed_columns(connection, engine, staging_table, pending)
                _insert_batch(connection, staging_table, pending)
                loaded_rows += len(pending)
        finally:
            cursor.close()

        _create_indexes(connection, engine, source_collection, staging_table, token)
        staged_rows = connection.execute(
            text(
                f"SELECT count(*) FROM {_quote(engine, schema_name)}."
                f"{_quote(engine, staging_name)}"
            )
        ).scalar_one()
        if loaded_rows != expected_rows or staged_rows != expected_rows:
            raise RuntimeError(
                f"Row count mismatch for {database_name}.{collection_name}: "
                f"MongoDB={expected_rows}, inserted={loaded_rows}, PostgreSQL={staged_rows}"
            )

        target_qualified = f"{_quote(engine, schema_name)}.{_quote(engine, table_name)}"
        stage_qualified = f"{_quote(engine, schema_name)}.{_quote(engine, staging_name)}"
        connection.execute(text(f"DROP TABLE IF EXISTS {target_qualified}"))
        connection.execute(
            text(
                f"ALTER TABLE {stage_qualified} RENAME TO "
                f"{_quote(engine, table_name)}"
            )
        )

    print(f"  Verified {staged_rows:,} PostgreSQL rows")
    return expected_rows, staged_rows


def _migrate_view(engine: Engine, source_database: Any, database_name: str, view_name: str) -> bool:
    schema_name = _source_schema(database_name)
    view_sql = {
        ("historical_data", "theme_top10_daily_metrics_2"): """
            SELECT d.collected_at, d.theme, d.platform, d.median_viral_score,
                   d.avg_viral_score, d.avg_views, d.median_views,
                   d.avg_engagement_rate, d.median_engagement_rate
            FROM public.theme_daily_metrics_2 AS d
            JOIN public.theme_rankings_2 AS r
              ON r.theme = d.theme
             AND r.platform = d.platform
             AND r.platform = 'All'
             AND r.rank <= 10
        """,
        ("realtime_data", "query_5_platform_overlap_top20"): """
            WITH platform_counts AS (
                SELECT theme, platform, SUM(appeared_times) AS appeared_times
                FROM realtime_data.query_5_platform_overlap_new
                GROUP BY theme, platform
            ), ranked_themes AS (
                SELECT theme
                FROM platform_counts
                GROUP BY theme
                ORDER BY SUM(appeared_times) DESC
                LIMIT 20
            )
            SELECT ranked.theme, platforms.platform,
                   COALESCE(counts.appeared_times, 0) AS appeared_times
            FROM ranked_themes AS ranked
            CROSS JOIN (VALUES ('YouTube'), ('TikTok')) AS platforms(platform)
            LEFT JOIN platform_counts AS counts
              ON counts.theme = ranked.theme
             AND counts.platform = platforms.platform
        """,
    }
    definition = view_sql.get((database_name, view_name))
    if definition is None:
        print(f"Skipping unsupported MongoDB view {database_name}.{view_name}")
        return False

    with engine.begin() as connection:
        connection.execute(
            text(
                f"CREATE OR REPLACE VIEW {_quote(engine, schema_name)}."
                f"{_quote(engine, normalize_identifier(view_name))} AS {definition}"
            )
        )
        postgres_rows = connection.execute(
            text(
                f"SELECT count(*) FROM {_quote(engine, schema_name)}."
                f"{_quote(engine, normalize_identifier(view_name))}"
            )
        ).scalar_one()

    mongo_rows = source_database[view_name].count_documents({})
    if postgres_rows != mongo_rows:
        raise RuntimeError(
            f"View row count mismatch for {database_name}.{view_name}: "
            f"MongoDB={mongo_rows}, PostgreSQL={postgres_rows}"
        )
    print(f"Verified MongoDB view {database_name}.{view_name}: {postgres_rows:,} rows")
    return True


def _insert_batch(connection: Any, table: Table, flattened_documents: list[dict[str, Any]]) -> None:
    buffer = StringIO()
    for document in flattened_documents:
        fields = []
        for column in table.columns:
            value = _coerce(document.get(column.name), column.type)
            if value is None:
                fields.append(r"\N")
                continue
            if isinstance(column.type, JSONB):
                value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
            elif isinstance(value, (date, datetime)):
                value = value.isoformat()
            elif isinstance(value, Decimal):
                value = str(value)
            elif isinstance(value, bool):
                value = "true" if value else "false"
            else:
                value = str(value)
            fields.append('"' + value.replace('"', '""') + '"')
        buffer.write(",".join(fields))
        buffer.write("\n")
    buffer.seek(0)

    raw_connection = connection.connection.driver_connection
    columns = ", ".join(_quote(connection.engine, column.name) for column in table.columns)
    statement = (
        f"COPY {_quote(connection.engine, table.schema)}.{_quote(connection.engine, table.name)} "
        f"({columns}) FROM STDIN WITH (FORMAT CSV, NULL E'\\\\N')"
    )
    with raw_connection.cursor() as cursor:
        cursor.copy_expert(statement, buffer)


def main() -> None:
    print(
        "WARNING: this replaces destination table contents. Do not run this migration "
        "after any application starts writing directly to PostgreSQL."
    )
    mongo_uri = os.environ["MONGO_URI"]
    postgres_url = URL.create(
        "postgresql+psycopg2",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ["POSTGRES_DB"],
    )

    mongo_client = MongoClient(mongo_uri)
    engine = create_engine(postgres_url, pool_pre_ping=True)
    try:
        database_names = [
            name for name in mongo_client.list_database_names() if name not in SYSTEM_DATABASES
        ]
        total_source_rows = 0
        total_postgres_rows = 0
        migrated_views = 0
        pending_views: list[tuple[str, Any, str]] = []
        for database_name in database_names:
            source_database = mongo_client[database_name]
            for item in source_database.list_collections():
                if item["name"].startswith("system."):
                    continue
                if item.get("type") == "view":
                    pending_views.append((database_name, source_database, item["name"]))
                    continue
                source_rows, postgres_rows = _migrate_collection(
                    engine, source_database, database_name, item["name"]
                )
                total_source_rows += source_rows
                total_postgres_rows += postgres_rows

        for database_name, source_database, view_name in pending_views:
            migrated_views += _migrate_view(engine, source_database, database_name, view_name)

        if total_source_rows != total_postgres_rows:
            raise RuntimeError(
                f"Overall row count mismatch: MongoDB={total_source_rows}, "
                f"PostgreSQL={total_postgres_rows}"
            )
        print(
            f"Migration complete. Verified {total_postgres_rows:,} rows across "
            f"{len(database_names)} databases and {migrated_views} derived views."
        )
    finally:
        mongo_client.close()
        engine.dispose()


if __name__ == "__main__":
    main()