package com.example.kafka_streams_examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/** Zajednička konfiguracija i životni ciklus za obe aplikacije. */
final class StreamsSupport {

    private static final Logger log = LoggerFactory.getLogger(StreamsSupport.class);

    private StreamsSupport() {
    }

    static Properties baseProperties(String defaultApplicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfig.env("APPLICATION_ID", defaultApplicationId));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.required("BOOTSTRAP_SERVERS"));
        props.put(StreamsConfig.STATE_DIR_CONFIG, AppConfig.env("STATE_DIR", "/app/kafka-streams"));
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, AppConfig.envInt("NUM_STREAM_THREADS", 1));
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, AppConfig.envInt("COMMIT_INTERVAL_MS", 5000));
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, AppConfig.envInt("STREAMS_REPLICATION_FACTOR", 2));
        // at_least_once ili exactly_once_v2 (exactly_once_v2 traži podešene
        // transaction.state.log.* na brokerima)
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, AppConfig.env("PROCESSING_GUARANTEE", "at_least_once"));
        props.put(
                StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                AppConfig.env("AUTO_OFFSET_RESET", "earliest"));

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // Neispravna (poison pill) poruka se loguje i preskače, ne ruši aplikaciju.
        props.put(
                "deserialization.exception.handler",
                "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");

        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedRocksDbConfig.class);
        return props;
    }

    /**
     * Pokreće topologiju i blokira do gašenja. Fatalna greška gasi proces (docker
     * restart ga podiže).
     */
    static void run(Topology topology, Properties props) {
        log.info("Topology:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger exitCode = new AtomicInteger(0);

        streams.setStateListener((newState, oldState) -> {
            log.info("Kafka Streams state: {} -> {}", oldState, newState);
            if (newState == KafkaStreams.State.ERROR) {
                exitCode.set(1);
                done.countDown();
            }
        });
        streams.setUncaughtExceptionHandler(ex -> {
            log.error("Stream thread failed, shutting the application down", ex);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });
        Runtime.getRuntime()
                .addShutdownHook(new Thread(
                        () -> {
                            streams.close(Duration.ofSeconds(20));
                            done.countDown();
                        },
                        "streams-shutdown-hook"));

        streams.start();
        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        streams.close(Duration.ofSeconds(10));
        if (exitCode.get() != 0) {
            System.exit(exitCode.get());
        }
    }
}