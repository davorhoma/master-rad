package com.example.kafka_streams_examples;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Terminalni procesor: svaki rezultat topologije upisuje u PostgreSQL (INSERT
 * ... ON CONFLICT DO UPDATE).
 *
 * Isporuka je at-least-once, ali je upis idempotentan nad prirodnim ključem,
 * pa ponovno procesiranje posle restarta daje isto stanje tabele.
 */
final class PgUpsertProcessor<V> implements Processor<String, V, Void, Void> {

    @FunctionalInterface
    interface RowWriter<V> {
        void write(Connection connection, V value) throws SQLException;
    }

    private final PostgresConfig config;
    private final List<String> ddl;
    private final RowWriter<V> writer;
    private PostgresClient client;

    PgUpsertProcessor(PostgresConfig config, List<String> ddl, RowWriter<V> writer) {
        this.config = config;
        this.ddl = ddl;
        this.writer = writer;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        client = new PostgresClient(config, ddl);
        client.ensureSchema();
    }

    @Override
    public void process(Record<String, V> record) {
        V value = record.value();
        if (value != null) {
            client.execute(connection -> writer.write(connection, value));
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}