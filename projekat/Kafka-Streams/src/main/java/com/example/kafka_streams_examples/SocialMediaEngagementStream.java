package com.example.kafka_streams_examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.bson.Document;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class SocialMediaEngagementStream {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        System.out.println("Started.");
        // Konfiguracija za Kafka Streams
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "social-media-engagement-processor-v2");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092"));
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // MongoDB konekcija (uzimamo iz environment promenljivih, baš kao u Airflow/Spark-u)
        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://admin:admin@mongodb:27017");
        String mongoDbName = System.getenv().getOrDefault("MONGO_DB", "realtime_data");
        String mongoCollectionName = System.getenv().getOrDefault("MONGO_COLLECTION", "query_7_engagement_over_time");

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(mongoDbName);
        MongoCollection<Document> collection = database.getCollection(mongoCollectionName);

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Čitanje sa YouTube topika
        KStream<String, String> ytStream = builder.stream("yt-monitoring-topic");
        // 2. Čitanje sa TikTok topika
        KStream<String, String> ttStream = builder.stream("tt-monitoring-topic");

        // Obrada YouTube poruka
        KStream<String, Document> ytProcessed = ytStream.mapValues(value -> {
            try {
                JsonNode node = objectMapper.readTree(value);
                long views = node.path("views").asLong(0);
                long likes = node.path("likes").asLong(0);
                long comments = node.path("comments").asLong(0);
                
                double engagement = views > 0 ? (double) (likes + comments) / views : 0.0;

                String collectedAt = node.has("collected_at") ? node.path("collected_at").asText() : Instant.now().toString();

                Document doc = new Document("platform", "YouTube")
                        .append("video_id", node.path("video_id").asText())
                        .append("title", node.path("title").asText())
                        .append("views", views)
                        .append("likes", likes)
                        .append("comments", comments)
                        .append("engagement_rate", engagement)
                        .append("timestamp", collectedAt);
                return doc;
            } catch (Exception e) {
                return null;
            }
        }).filter((key, value) -> value != null);

        // Obrada TikTok poruka
        KStream<String, Document> ttProcessed = ttStream.mapValues(value -> {
            try {
                JsonNode node = objectMapper.readTree(value);
                long views = node.path("views").asLong(0);
                long likes = node.path("likes").asLong(0);
                long comments = node.path("comments").asLong(0);
                
                double engagement = views > 0 ? (double) (likes + comments) / views : 0.0;

                String collectedAt = node.has("trending_detected_time") ? node.path("trending_detected_time").asText() : Instant.now().toString();

                Document doc = new Document("platform", "TikTok")
                        .append("video_id", node.path("video_id").asText())
                        .append("title", node.path("description").asText()) // TikTok koristi description umesto title
                        .append("views", views)
                        .append("likes", likes)
                        .append("comments", comments)
                        .append("engagement_rate", engagement)
                        .append("timestamp", collectedAt);
                return doc;
            } catch (Exception e) {
                return null;
            }
        }).filter((key, value) -> value != null);

        // Spajanje (Merge) oba toka u jedan zajednički tok
        KStream<String, Document> mergedStream = ytProcessed.merge(ttProcessed);

        // Upisivanje svakog izračunatog dokumenta direktno u MongoDB (Sink)
        mergedStream.foreach((key, doc) -> {
            try {
                collection.insertOne(doc);
                System.out.println("Uspešno upisano u MongoDB: " + doc.toJson());
            } catch (Exception e) {
                System.err.println("Greška pri upisu u MongoDB: " + e.getMessage());
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        
        // Gašenje aplikacije uz oslobađanje Mongo resursa
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            mongoClient.close();
        }));

        streams.start();

        System.out.println("Finished.");
    }
}