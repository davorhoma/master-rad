import os
from urllib.parse import urlsplit

from pyspark.sql import DataFrame


def write_postgres(
    frame: DataFrame,
    jdbc_url: str,
    schema: str,
    table: str,
    preserve_existing_indexes: bool = True,
) -> None:
    writer = (
        frame.write.format("jdbc")
        .mode("overwrite")
        .option("url", jdbc_url)
        .option("dbtable", f'"{schema}"."{table}"')
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
    )
    if preserve_existing_indexes:
        writer = writer.option("truncate", "true")
    writer.save()


def _quote_identifier(identifier: str) -> str:
    """Quote a PostgreSQL identifier safely."""
    return '"' + identifier.replace('"', '""') + '"'


def upsert_postgres(
    frame: DataFrame,
    jdbc_url: str,
    schema: str,
    table: str,
    key_columns: list[str],
    column_types: dict[str, str],
) -> None:
    """Create the target table if needed, then insert or update rows."""

    import psycopg2
    from urllib.parse import urlsplit

    if not key_columns:
        raise ValueError("key_columns must contain at least one column")

    missing_columns = [name for name in frame.columns if name not in column_types]
    if missing_columns:
        raise ValueError(f"Missing PostgreSQL types for columns: {missing_columns}")

    missing_keys = [key for key in key_columns if key not in frame.columns]
    if missing_keys:
        raise ValueError(f"Key columns missing from DataFrame: {missing_keys}")

    def quote(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    parsed_url = urlsplit(jdbc_url.removeprefix("jdbc:"))
    if not parsed_url.hostname or not parsed_url.path.strip("/"):
        raise ValueError(f"Unsupported PostgreSQL JDBC URL: {jdbc_url}")

    pg_database = parsed_url.path.lstrip("/").split("/")[0]
    pg_host = parsed_url.hostname
    pg_port = parsed_url.port or 5432

    target = f"{quote(schema)}.{quote(table)}"
    staging_name = f"{table}_staging"
    staging = f"{quote(schema)}.{quote(staging_name)}"

    column_defs = []
    for name in frame.columns:
        definition = f"{quote(name)} {column_types[name]}"
        if name in key_columns:
            definition += " NOT NULL"
        column_defs.append(definition)

    create_table_sql = f"CREATE TABLE IF NOT EXISTS {target} " f"({', '.join(column_defs)})"

    columns_sql = ", ".join(quote(name) for name in frame.columns)
    key_sql = ", ".join(quote(name) for name in key_columns)

    update_columns = [name for name in frame.columns if name not in key_columns]

    if update_columns:
        update_sql = ", ".join(f"{quote(name)} = EXCLUDED.{quote(name)}" for name in update_columns)
        conflict_action = f"DO UPDATE SET {update_sql}"
    else:
        conflict_action = "DO NOTHING"

    index_name = f"{table}_{'_'.join(key_columns)}_uq"
    create_index_sql = (
        f"CREATE UNIQUE INDEX IF NOT EXISTS {quote(index_name)} " f"ON {target} ({key_sql})"
    )

    upsert_sql = f"""
        INSERT INTO {target} ({columns_sql})
        SELECT {columns_sql}
        FROM {staging}
        ON CONFLICT ({key_sql}) {conflict_action}
    """

    # Only the staging table is overwritten.
    (
        frame.write.format("jdbc")
        .mode("overwrite")
        .option("url", jdbc_url)
        .option("dbtable", staging)
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "5000")
        .save()
    )

    connection = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_database,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                cursor.execute(create_index_sql)
                cursor.execute(upsert_sql)
                cursor.execute(f"DROP TABLE IF EXISTS {staging}")
    finally:
        connection.close()


def ensure_postgres_table(
    jdbc_url: str,
    schema: str,
    table: str,
    template_table: str,
) -> None:
    """Create the target table using a staging table's structure if needed."""
    import psycopg2
    from urllib.parse import urlsplit

    def quote(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    parsed_url = urlsplit(jdbc_url.removeprefix("jdbc:"))
    if not parsed_url.hostname or not parsed_url.path.strip("/"):
        raise ValueError(f"Unsupported PostgreSQL JDBC URL: {jdbc_url}")

    connection = psycopg2.connect(
        host=parsed_url.hostname,
        port=parsed_url.port or 5432,
        dbname=parsed_url.path.lstrip("/").split("/")[0],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    target = f"{quote(schema)}.{quote(table)}"
    template = f"{quote(schema)}.{quote(template_table)}"

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {target}
                    (LIKE {template} INCLUDING ALL)
                    """)
    finally:
        connection.close()
