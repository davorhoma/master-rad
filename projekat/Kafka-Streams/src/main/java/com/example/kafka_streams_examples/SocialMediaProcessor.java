package com.example.kafka_streams_examples;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SocialMediaProcessor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        System.out.println("Started main");
        // 1. Čitanje environment promenljivih prosleđenih iz docker-compose.yml
        String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092");
        String ytMonitoringTopic = System.getenv().getOrDefault("YT_MONITORING_TOPIC", "yt-monitoring-topic");
        String ttMonitoringTopic = System.getenv().getOrDefault("TT_MONITORING_TOPIC", "tt-monitoring-topic");
        String ytSearchTopic = System.getenv().getOrDefault("YT_SEARCH_TOPIC", "yt-search-topic");
        String ttSearchTopic = System.getenv().getOrDefault("TT_SEARCH_TOPIC", "tt-search-topic");

        // 2. Konfiguracija Kafka Streams aplikacije
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "social-media-stream-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // 3. Kreiranje stream-ova za sva 4 topic-a (uz korišćenje Consumed Serdes-a kao
        // u asistentovom primeru)
        KStream<String, String> ytMonitoringStream = builder.stream(ytMonitoringTopic,
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> ttMonitoringStream = builder.stream(ttMonitoringTopic,
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> ytSearchStream = builder.stream(ytSearchTopic,
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> ttSearchStream = builder.stream(ttSearchTopic,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Primer obrade za YouTube Monitoring (možeš dodati parsiranje JSON-a preko
        // Jackson-a kao kod asistenta)
        ytMonitoringStream.peek((key, value) -> {
            try {
                JsonNode root = MAPPER.readTree(value);
                System.out.println("[YT-MONITORING] Primljen podatak: " + root.toString());
                // Ovde kasnije dodaješ logiku transformacije, validacije ili slanja u
                // HDFS/Mongodbu
            } catch (Exception e) {
                System.err.println("[YT-MONITORING] Greška pri parsiranju JSON-a: " + e.getMessage());
            }
        });

        // Primer obrade za TikTok Monitoring
        ttMonitoringStream.peek((key, value) -> {
            try {
                JsonNode root = MAPPER.readTree(value);
                System.out.println("[TT-MONITORING] Primljen podatak: " + root.toString());
            } catch (Exception e) {
                System.err.println("[TT-MONITORING] Greška pri parsiranju JSON-a: " + e.getMessage());
            }
        });

        // Isto možeš uraditi i za search stream-ove...
        ytSearchStream.peek((key, value) -> System.out.println("[YT-SEARCH] Primljeno: " + value));
        ttSearchStream.peek((key, value) -> System.out.println("[TT-SEARCH] Primljeno: " + value));

        // 4. Pokretanje Kafka Streams topologije
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}