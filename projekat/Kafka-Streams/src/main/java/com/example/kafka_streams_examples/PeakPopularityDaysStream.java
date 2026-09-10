package com.example.kafka_streams_examples;

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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.bson.Document;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class PeakPopularityDaysStream {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature());

    public static class VideoPayload {
        public String platform;
        public String videoId;
        public String theme;
        public String publishTime;
        public long views;
        public long detectedTimestamp;

        public VideoPayload() {
        }

        public VideoPayload(String platform, String videoId, String theme, String publishTime, long views,
                long detectedTimestamp) {
            this.platform = platform;
            this.videoId = videoId;
            this.theme = theme;
            this.publishTime = publishTime;
            this.views = views;
            this.detectedTimestamp = detectedTimestamp;
        }
    }

    public static class VideoPeakState {
        public String platform;
        public String videoId;
        public String theme;
        public String publishTime;
        public long maxViews;
        public long peakDetectedTimestamp;
        public double daysToPeak;

        public VideoPeakState() {
            this.maxViews = -1L;
        }

        public VideoPeakState(String platform, String videoId, String theme, String publishTime, long maxViews,
                long peakDetectedTimestamp, double daysToPeak) {
            this.platform = platform;
            this.videoId = videoId;
            this.theme = theme;
            this.publishTime = publishTime;
            this.maxViews = maxViews;
            this.peakDetectedTimestamp = peakDetectedTimestamp;
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
        System.out.println("Started Peak Popularity Days Processor.");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "peak-popularity-days-processor-v7");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092"));
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 10 * 1024 * 1024L);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://admin:admin@mongodb:27017");
        String mongoDbName = System.getenv().getOrDefault("MONGO_DB", "realtime_data");
        String mongoCollectionName = System.getenv().getOrDefault("MONGO_COLLECTION", "query_4_peak_popularity_days");

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(mongoDbName);
        MongoCollection<Document> collection = database.getCollection(mongoCollectionName);
        System.out.println("Successfully connected to MongoDB database: " + mongoDbName);

        StreamsBuilder builder = new StreamsBuilder();
        AtomicLong messageCounter = new AtomicLong(0);
        AtomicLong mongoWriteCounter = new AtomicLong(0);

        KStream<String, String> ytMonitoringStream = builder.stream("yt-monitoring-topic");
        KStream<String, String> ytSearchStream = builder.stream("yt-search-topic");
        KStream<String, String> ttSearchStream = builder.stream("tt-search-topic");

        KStream<String, String> inputStream = ytMonitoringStream
                .merge(ytSearchStream)
                .merge(ttSearchStream);

        KTable<String, VideoPeakState> peakStateTable = inputStream
                .flatMapValues(value -> {
                    try {
                        JsonNode node = objectMapper.readTree(value);

                        String videoId = node.has("video_id") ? node.path("video_id").asText() : null;
                        if (videoId == null || videoId.isEmpty()) {
                            return java.util.Collections.emptyList();
                        }

                        String platform = node.has("region_code") || node.has("channel_id") ? "YouTube" : "TikTok";
                        long views = node.has("views") ? node.path("views").asLong(0) : 0;

                        String publishTimeStr = node.has("publish_time") ? node.path("publish_time").asText(null)
                                : (node.has("published_at") ? node.path("published_at").asText(null) : null);

                        String detectedTimeStr = node.has("trending_detected_time")
                                ? node.path("trending_detected_time").asText(null)
                                : (node.has("collected_at") ? node.path("collected_at").asText(null)
                                        : Instant.now().toString());

                        if (publishTimeStr != null) {
                            long detectedTimestamp = Instant.parse(detectedTimeStr).toEpochMilli();
                            VideoPayload payload = new VideoPayload(platform, videoId, "gaming", publishTimeStr, views,
                                    detectedTimestamp);
                            return java.util.Collections.singletonList(payload);
                        }
                    } catch (Exception ignored) {
                    }
                    return java.util.Collections.emptyList();
                })
                .groupBy(
                        (key, payload) -> payload.platform + "_" + payload.videoId,
                        Grouped.with(Serdes.String(), new JsonSerde<>(VideoPayload.class)))
                .aggregate(
                        VideoPeakState::new,
                        (aggKey, newPayload, aggregate) -> {
                            // Inicijalizacija ako je prvi zapis ili ako novi zapis ima veći broj pregleda
                            // (ili isto pregleda ali noviji timestamp)
                            if (aggregate.videoId == null || newPayload.views > aggregate.maxViews
                                    || (newPayload.views == aggregate.maxViews
                                            && newPayload.detectedTimestamp >= aggregate.peakDetectedTimestamp)) {

                                Instant publishInstant = Instant.parse(newPayload.publishTime);
                                Instant peakInstant = Instant.ofEpochMilli(newPayload.detectedTimestamp);

                                double daysElapsed = Duration.between(publishInstant, peakInstant).toMillis()
                                        / (1000.0 * 60 * 60 * 24);
                                if (daysElapsed < 0)
                                    daysElapsed = 0.0;

                                return new VideoPeakState(
                                        newPayload.platform,
                                        newPayload.videoId,
                                        newPayload.theme,
                                        newPayload.publishTime,
                                        newPayload.views,
                                        newPayload.detectedTimestamp,
                                        daysElapsed);
                            }
                            return aggregate;
                        },
                        Materialized.<String, VideoPeakState, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(
                                "video-peak-state-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(VideoPeakState.class)));

        // Upis u MongoDB sa praćenjem metrika
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

                long totalMessages = messageCounter.incrementAndGet();
                long totalWrites = mongoWriteCounter.incrementAndGet();

                if (totalMessages % 1000 == 0 || totalWrites % 1000 == 0) {
                    System.out.println("Obrađeno poruka: " + totalMessages +
                            " | Ažuriranih vrhunaca u MongoDB: " + totalWrites +
                            " | Video ID: " + state.videoId +
                            " | Max Views: " + state.maxViews +
                            " | Dana do maksimuma: " + (Math.round(state.daysToPeak * 100.0) / 100.0));
                }

            } catch (Exception e) {
                System.err.println("[MONGODB_ERROR] " + e.getMessage());
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                mongoClient.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            System.out.println("Peak Popularity Stream Processor Started Successfully.");
            latch.await();
        } catch (Throwable e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }
}