package com.example.kafka_streams_examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

/**
 * Jedna JDBC konekcija sa retry/backoff logikom.
 *
 * - prolazne greške (veza pala, restart baze): reconnect + eksponencijalni
 * backoff
 * - 42P01 (tabela ne postoji) / 42P10 (nema unique indeksa za ON CONFLICT):
 * ponovo se izvršava DDL
 * (npr. kada batch job obriše i ponovo kreira tabelu)
 * - greške u podacima (SQLState 22xxx/23xxx): red se loguje i preskače, da
 * jedna loša vrednost ne zaglavi stream
 * - ako se ništa ne popravi posle MAX_ATTEMPTS pokušaja, baca izuzetak
 * (aplikacija se gasi, docker je restartuje)
 */
final class PostgresClient implements AutoCloseable {

    @FunctionalInterface
    interface SqlAction {
        void run(Connection connection) throws SQLException;
    }

    private static final Logger log = LoggerFactory.getLogger(PostgresClient.class);
    private static final int MAX_ATTEMPTS = 8;

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("PostgreSQL JDBC driver is not on the classpath", e);
        }
    }

    private final PostgresConfig config;
    private final List<String> ddl;
    private Connection connection;

    PostgresClient(PostgresConfig config, List<String> ddl) {
        this.config = config;
        this.ddl = ddl;
    }

    void ensureSchema() {
        run(
                conn -> {
                    for (String statement : ddl) {
                        try (Statement st = conn.createStatement()) {
                            st.execute(statement);
                        }
                    }
                },
                false);
    }

    void execute(SqlAction action) {
        run(action, true);
    }

    private void run(SqlAction action, boolean repairSchemaOnError) {
        SQLException last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                action.run(connection());
                return;
            } catch (SQLException e) {
                last = e;
                String state = e.getSQLState();

                if (state != null && (state.startsWith("22") || state.startsWith("23"))) {
                    log.error("Skipping row rejected by PostgreSQL (SQLState {}): {}", state, e.getMessage());
                    return;
                }

                resetConnection();

                if (repairSchemaOnError && ("42P01".equals(state) || "42P10".equals(state))) {
                    log.warn("Schema problem (SQLState {}), re-running DDL for {}", state, config.qualifiedTable());
                    ensureSchema();
                    continue;
                }

                long sleepMs = Math.min(30_000L, 1_000L << (attempt - 1));
                log.warn(
                        "PostgreSQL error (attempt {}/{}, SQLState {}): {}. Retrying in {} ms",
                        attempt,
                        MAX_ATTEMPTS,
                        state,
                        e.getMessage(),
                        sleepMs);
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while retrying PostgreSQL write", ie);
                }
            }
        }
        throw new IllegalStateException("PostgreSQL write failed after " + MAX_ATTEMPTS + " attempts", last);
    }

    private Connection connection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            Properties props = new Properties();
            props.setProperty("user", config.user());
            props.setProperty("password", config.password());
            props.setProperty("ApplicationName", "kafka-streams-" + config.table());
            props.setProperty("connectTimeout", "10");
            props.setProperty("socketTimeout", "30");
            props.setProperty("tcpKeepAlive", "true");
            connection = DriverManager.getConnection(config.jdbcUrl(), props);
            connection.setAutoCommit(true);
        }
        return connection;
    }

    private void resetConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // veza je ionako neispravna
            }
            connection = null;
        }
    }

    @Override
    public void close() {
        resetConnection();
    }
}