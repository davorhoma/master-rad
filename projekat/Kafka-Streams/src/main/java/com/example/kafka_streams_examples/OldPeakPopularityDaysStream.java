package com.example.kafka_streams_examples;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.bson.Document;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReturnDocument;

public class OldPeakPopularityDaysStream {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature());

    public static class VideoPayload {
        public String platform;
        public String videoId;
        public String theme;
        public String publishTime;
        public long views;
        public double daysToPeak;

        public VideoPayload() {
        }

        public VideoPayload(String platform, String videoId, String theme, String publishTime, long views,
                double daysToPeak) {
            this.platform = platform;
            this.videoId = videoId;
            this.theme = theme;
            this.publishTime = publishTime;
            this.views = views;
            this.daysToPeak = daysToPeak;
        }
    }

    public static class VideoPeakState {
        public String platform;
        public String videoId;
        public String theme;
        public String publishTime;
        public long maxViews;
        public double daysToPeak;

        public VideoPeakState() {
        }

        public VideoPeakState(String platform, String videoId, String theme, String publishTime, long maxViews,
                double daysToPeak) {
            this.platform = platform;
            this.videoId = videoId;
            this.theme = theme;
            this.publishTime = publishTime;
            this.maxViews = maxViews;
            this.daysToPeak = daysToPeak;
        }
    }

    public static class JsonSerde<T> implements Serde<T> {
        private final Class<T> targetClass;

        public JsonSerde(Class<T> targetClass) {
            this.targetClass = targetClass;
        }

        @Override
        public Serializer<T> serializer() {
            return (topic, data) -> {
                try {
                    return objectMapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new RuntimeException("Error serializing JSON", e);
                }
            };
        }

        @Override
        public Deserializer<T> deserializer() {
            return (topic, bytes) -> {
                if (bytes == null)
                    return null;
                try {
                    return objectMapper.readValue(bytes, targetClass);
                } catch (Exception e) {
                    throw new RuntimeException("Error deserializing JSON", e);
                }
            };
        }
    }

    public static void main(String[] args) {
        System.out.println("Started Peak Popularity Days Processor for PP4.");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "peak-popularity-days-processor-v2");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092"));
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // MongoDB konekcija
        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://admin:admin@mongodb:27017");
        String mongoDbName = System.getenv().getOrDefault("MONGO_DB", "realtime_data");
        String mongoCollectionName = System.getenv().getOrDefault("MONGO_COLLECTION", "query_4_peak_popularity_days");

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(mongoDbName);
        MongoCollection<Document> collection = database.getCollection(mongoCollectionName);
        System.out.println(
                "Successfully connected to MongoDB database: " + mongoDbName + ", collection: " + mongoCollectionName);

        StreamsBuilder builder = new StreamsBuilder();

        AtomicLong totalMessageCounter = new AtomicLong(0);

        // Čitanje sa sva 4 topika istovremeno
        KStream<String, String> ytMonitoring = builder.stream("yt-monitoring-topic");
        KStream<String, String> ytSearch = builder.stream("yt-search-topic");
        KStream<String, String> ttMonitoring = builder.stream("tt-monitoring-topic");
        KStream<String, String> ttSearch = builder.stream("tt-search-topic");

        // Unified Stream svih 4 topika sa logovanjem prijema
        KStream<String, VideoPayload> allVideosStream = ytMonitoring.merge(ytSearch)
                .merge(ttMonitoring)
                .merge(ttSearch)
                .peek((k, v) -> {
                    long count = totalMessageCounter.incrementAndGet();
                    if (count % 10 == 0 || count == 1) {
                        System.out
                                .println("[KAFKA_STREAM] Primljena ukupno " + count + "-ta poruka. Sadržaj (skraćeno): "
                                        + (v != null && v.length() > 150 ? v.substring(0, 150) + "..." : v));
                    }
                })
                .flatMap((key, value) -> {
                    List<KeyValue<String, VideoPayload>> result = new ArrayList<>();
                    try {
                        JsonNode node = objectMapper.readTree(value);

                        String platform = node.has("region_code") || node.has("channel_id") ? "YouTube" : "TikTok";

                        String videoId = node.has("video_id") ? node.path("video_id").asText() : null;
                        if (videoId == null || videoId.isEmpty()) {
                            System.err.println("[PARSER_WARNING] Poruka nema video_id! Preskačem.");
                            return result;
                        }

                        long views = node.has("views") ? node.path("views").asLong(0) : 0;

                        String publishTimeStr = node.has("publish_time") ? node.path("publish_time").asText(null)
                                : (node.has("published_at") ? node.path("published_at").asText(null) : null);

                        String detectedTimeStr = node.has("trending_detected_time")
                                ? node.path("trending_detected_time").asText(null)
                                : (node.has("collected_at") ? node.path("collected_at").asText(null)
                                        : Instant.now().toString());

                        if (publishTimeStr != null) {
                            Instant publishInstant = Instant.parse(publishTimeStr);
                            Instant detectedInstant = Instant.parse(detectedTimeStr);

                            double daysElapsed = Duration.between(publishInstant, detectedInstant).toMillis()
                                    / (1000.0 * 60 * 60 * 24);
                            if (daysElapsed < 0)
                                daysElapsed = 0.0;

                            String theme = "gaming";

                            VideoPayload payload = new VideoPayload(platform, videoId, theme, publishTimeStr, views,
                                    daysElapsed);
                            String mapKey = platform + "_" + videoId;
                            result.add(new KeyValue<>(mapKey, payload));
                            // System.out.println("[PARSER_SUCCESS] Uspešno obrađen video: " + videoId + " |
                            // Platforma: " + platform + " | Views: " + views);
                        } else {
                            System.err.println("[PARSER_WARNING] Video " + videoId
                                    + " nema publish_time/published_at! Presкачем.");
                        }
                    } catch (Exception e) {
                        System.err.println("[PARSER_ERROR] Greшка при парсирању JSON-а: " + e.getMessage()
                                + " | Vredност: " + value);
                    }
                    return result;
                });

        // KTable za praćenje maksimalnog broja pregleda uz logovanje agregције
        KTable<String, VideoPeakState> peakStateTable = allVideosStream
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(VideoPayload.class)))
                .aggregate(
                        VideoPeakState::new,
                        (aggKey, newPayload, aggregate) -> {
                            if (aggregate.videoId == null) {
                                System.out.println("[AGGREGATE_INIT] Inicijalno stanje za video: " + newPayload.videoId
                                        + " | MaxViews: " + newPayload.views);
                                return new VideoPeakState(
                                        newPayload.platform,
                                        newPayload.videoId,
                                        newPayload.theme,
                                        newPayload.publishTime,
                                        newPayload.views,
                                        newPayload.daysToPeak);
                            }

                            if (newPayload.views > aggregate.maxViews) {
                                System.out.println("[AGGREGATE_UPDATE] Pronađen novi PEAK za video: "
                                        + newPayload.videoId + " | Stari max: " + aggregate.maxViews + " -> Novi max: "
                                        + newPayload.views);
                                aggregate.maxViews = newPayload.views;
                                aggregate.daysToPeak = newPayload.daysToPeak;
                            }
                            return aggregate;
                        },
                        Materialized.<String, VideoPeakState, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(
                                "video-peak-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(VideoPeakState.class)));

        // Upisivanje agregiranih rezultata u MongoDB uz logovanje
        peakStateTable.toStream().foreach((key, state) -> {
            try {
                if (state == null || state.videoId == null)
                    return;

                Document mongoDoc = new Document("platform", state.platform)
                        .append("video_id", state.videoId)
                        .append("theme", state.theme)
                        .append("publish_time", state.publishTime)
                        .append("max_views", state.maxViews)
                        .append("days_to_peak", Math.round(state.daysToPeak * 100.0) / 100.0);

                collection.findOneAndReplace(
                        Filters.and(
                                Filters.eq("platform", state.platform),
                                Filters.eq("video_id", state.videoId)),
                        mongoDoc,
                        new FindOneAndReplaceOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

                System.out.println("[MONGODB_SYNC] Uspeшно сачуван/ажуриран документ у бази за video_id: "
                        + state.videoId + " (max_views: " + state.maxViews + ")");

            } catch (Exception e) {
                System.err.println("[MONGODB_ERROR] Greška pri upisu u MongoDB za PP4: " + e.getMessage());
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Gašenje Kafka Streams-а и MongoDB конекције...");
                streams.close();
                mongoClient.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            System.out.println("Peak Popularity Days Processor Started Successfully.");
            latch.await();
        } catch (Throwable e) {
            System.err.println("Greška u radu aplikacije: " + e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }
}