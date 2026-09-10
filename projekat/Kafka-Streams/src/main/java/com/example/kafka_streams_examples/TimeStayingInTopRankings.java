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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.bson.Document;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

public class TimeStayingInTopRankings {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature());

    public static void main(String[] args) {
        System.out.println("Started Query 10 Aggregated Processor (Kafka Streams 4.x API).");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "time-staying-in-top-rankings-processor-v1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092"));
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // MongoDB konekcija
        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://admin:admin@mongodb:27017");
        String mongoDbName = System.getenv().getOrDefault("MONGO_DB", "realtime_data");
        String mongoCollectionName = System.getenv().getOrDefault("MONGO_COLLECTION", "query_10_ready_for_chart");

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(mongoDbName);
        MongoCollection<Document> collection = database.getCollection(mongoCollectionName);

        StreamsBuilder builder = new StreamsBuilder();

        // Naziv State Store-a
        String storeName = "video-duration-store";
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(storeName),
                        Serdes.String(),
                        Serdes.String()));

        // Čitanje sa YouTube i TikTok topika
        KStream<String, String> ytStream = builder.stream("yt-monitoring-topic");
        KStream<String, String> ttStream = builder.stream("tt-monitoring-topic");

        // Mapiranje YouTube u format [video_id, Document]
        KStream<String, Document> ytProcessed = ytStream.map((key, value) -> {
            try {
                JsonNode node = objectMapper.readTree(value);
                String videoId = node.has("video_id") ? node.path("video_id").asText() : null;
                if (videoId == null)
                    return new KeyValue<>(null, null);

                String timestampStr = node.has("collected_at") ? node.path("collected_at").asText()
                        : Instant.now().toString();

                Document doc = new Document("platform", "YouTube")
                        .append("video_id", videoId)
                        .append("timestamp", timestampStr);
                return new KeyValue<>(videoId, doc);
            } catch (Exception e) {
                System.err.println("Greška pri mapiranju JSON-a: " + e.getMessage());
                return new KeyValue<>(null, null);
            }
        }).filter((key, value) -> key != null && value != null);

        // Mapiranje TikTok u format [video_id, Document]
        KStream<String, Document> ttProcessed = ttStream.map((key, value) -> {
            try {
                JsonNode node = objectMapper.readTree(value);
                String videoId = node.has("video_id") ? node.path("video_id").asText()
                        : (node.has("id") ? node.path("id").asText() : null);
                if (videoId == null)
                    return new KeyValue<>(null, null);

                String timestampStr = node.has("trending_detected_time") ? node.path("trending_detected_time").asText()
                        : (node.has("collected_at") ? node.path("collected_at").asText() : Instant.now().toString());

                Document doc = new Document("platform", "TikTok")
                        .append("video_id", videoId)
                        .append("timestamp", timestampStr);
                return new KeyValue<>(videoId, doc);
            } catch (Exception e) {
                System.err.println("Greška pri mapiranju JSON-a: " + e.getMessage());
                return new KeyValue<>(null, null);
            }
        }).filter((key, value) -> key != null && value != null);

        // Spajanje tokova
        KStream<String, Document> mergedStream = ytProcessed.merge(ttProcessed);

        // Korišćenje novog Kafka Streams 4.x Processor API-ja
        mergedStream.process(new ProcessorSupplier<String, Document, Void, Void>() {
            @Override
            public Processor<String, Document, Void, Void> get() {
                return new Processor<String, Document, Void, Void>() {
                    private KeyValueStore<String, String> stateStore;

                    @Override
                    public void init(ProcessorContext<Void, Void> context) {
                        Processor.super.init(context);
                        this.stateStore = context.getStateStore(storeName);
                    }

                    @Override
                    public void process(Record<String, Document> record) {
                        try {
                            String videoId = record.key();
                            Document value = record.value();
                            if (videoId == null || value == null)
                                return;

                            String platform = value.getString("platform");
                            String currentTimestampStr = value.getString("timestamp");
                            LocalDate currentDate = OffsetDateTime.parse(currentTimestampStr).toLocalDate();

                            String existingDataJson = stateStore.get(videoId);
                            LocalDate minDate = currentDate;
                            LocalDate maxDate = currentDate;

                            if (existingDataJson != null) {
                                Document existingDoc = Document.parse(existingDataJson);
                                LocalDate storedMin = LocalDate.parse(existingDoc.getString("min_date"));
                                LocalDate storedMax = LocalDate.parse(existingDoc.getString("max_date"));

                                // Ispravljena logika za širenje opsega datuma
                                if (storedMin.isBefore(minDate)) {
                                    minDate = storedMin;
                                }
                                if (storedMax.isAfter(maxDate)) {
                                    maxDate = storedMax;
                                }
                            }

                            // Računanje razlike u danima u Javi
                            long daysInTop = ChronoUnit.DAYS.between(minDate, maxDate);

                            // Ažuriranje State Store-a
                            Document newStateDoc = new Document("min_date", minDate.toString())
                                    .append("max_date", maxDate.toString())
                                    .append("platform", platform);
                            stateStore.put(videoId, newStateDoc.toJson());

                            // Upsert u MongoDB
                            Document mongoDoc = new Document("video_id", videoId)
                                    .append("platform", platform)
                                    .append("days_in_top", (int) daysInTop);

                            collection.findOneAndReplace(
                                    Filters.eq("video_id", videoId),
                                    mongoDoc,
                                    new FindOneAndReplaceOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

                            // Ispis potvrde o upisu u konzolu
                            System.out.println("Upisano u MongoDB [Query 10]: Platform=" + platform +
                                    ", Video ID=" + videoId + ", Days in Top=" + (int) daysInTop);

                        } catch (Exception e) {
                            System.err.println("KRITIČNA GREŠKA U PROCESORU za video_id: " + record.key());
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        }, storeName);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            mongoClient.close();
        }));

        streams.start();
        System.out.println("Query 10 Aggregated Processor Started Successfully.");
    }
}