package com.example.kafka_streams_examples;

import java.util.regex.Pattern;

/** Konekcija i ciljna tabela, iz environment varijabli. */
record PostgresConfig(String jdbcUrl, String user, String password, String schema, String table) {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    PostgresConfig {
        quote(schema);
        quote(table);
    }

    static PostgresConfig fromEnv(String defaultTable) {
        String url = AppConfig.env("POSTGRES_JDBC_URL", null);
        if (url == null) {
            url = "jdbc:postgresql://"
                    + AppConfig.env("POSTGRES_HOST", "postgres")
                    + ":"
                    + AppConfig.env("POSTGRES_PORT", "5432")
                    + "/"
                    + AppConfig.required("POSTGRES_DB");
        }
        return new PostgresConfig(
                url,
                AppConfig.required("POSTGRES_USER"),
                AppConfig.required("POSTGRES_PASSWORD"),
                AppConfig.env("POSTGRES_SCHEMA", "public"),
                AppConfig.env("POSTGRES_TABLE", defaultTable));
    }

    String quotedSchema() {
        return quote(schema);
    }

    String qualifiedTable() {
        return quote(schema) + "." + quote(table);
    }

    static String quote(String identifier) {
        if (identifier == null || !IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Invalid PostgreSQL identifier: " + identifier);
        }
        return "\"" + identifier + "\"";
    }
}