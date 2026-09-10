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

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Properties;

public class SocialMediaDayTypeEngagementStream {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        System.out.println("Started Query 8 Processor.");

        // Konfiguracija za Kafka Streams sa jedinstvenim application.id
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "social-media-day-type-processor-v4");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092"));
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // MongoDB konekcija
        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://admin:admin@mongodb:27017");
        String mongoDbName = System.getenv().getOrDefault("MONGO_DB", "realtime_data");
        // Nova kolekcija za upit 8
        String mongoCollectionName = System.getenv().getOrDefault("MONGO_COLLECTION", "query_8_day_type_engagement");

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(mongoDbName);
        MongoCollection<Document> collection = database.getCollection(mongoCollectionName);

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Čitanje sa YouTube i TikTok topika
        KStream<String, String> ytStream = builder.stream("yt-monitoring-topic");
        KStream<String, String> ttStream = builder.stream("tt-monitoring-topic");

        // Obrada YouTube poruka
        KStream<String, Document> ytProcessed = ytStream.mapValues(value -> {
            try {
                JsonNode node = objectMapper.readTree(value);
                JsonNode viewsNode = node.path("views");
                long views = 0;
                if (viewsNode.isNumber()) {
                    views = viewsNode.asLong(0);
                } else if (viewsNode.isTextual()) {
                    try {
                        views = Long.parseLong(viewsNode.asText());
                    } catch (NumberFormatException e) {
                        views = 0;
                    }
                }

                // Uzimamo tačan trenutak prikupljanja (collected_at)
                String timestampStr = node.has("collected_at") ? node.path("collected_at").asText()
                        : Instant.now().toString();
                String dayType = determineDayType(timestampStr);

                Document doc = new Document("platform", "YouTube")
                        .append("day_type", dayType) // "Radni dan" ili "Vikend"
                        .append("views", views)
                        .append("timestamp", timestampStr);
                return doc;
            } catch (Exception e) {
                return null;
            }
        }).filter((key, value) -> value != null);

        // Obrada TikTok poruka
        KStream<String, Document> ttProcessed = ttStream.mapValues(value -> {
            try {
                JsonNode node = objectMapper.readTree(value);
                JsonNode viewsNode = node.path("views");
                long views = 0;
                if (viewsNode.isNumber()) {
                    views = viewsNode.asLong(0);
                } else if (viewsNode.isTextual()) {
                    try {
                        views = Long.parseLong(viewsNode.asText());
                    } catch (NumberFormatException e) {
                        views = 0;
                    }
                }

                // Za TikTok koristimo trending_detected_time ili collected_at
                String timestampStr = node.has("trending_detected_time") ? node.path("trending_detected_time").asText()
                        : (node.has("collected_at") ? node.path("collected_at").asText() : Instant.now().toString());
                String dayType = determineDayType(timestampStr);

                Document doc = new Document("platform", "TikTok")
                        .append("day_type", dayType)
                        .append("views", views)
                        .append("timestamp", timestampStr);
                return doc;
            } catch (Exception e) {
                return null;
            }
        }).filter((key, value) -> value != null);

        // Spajanje oba toka
        KStream<String, Document> mergedStream = ytProcessed.merge(ttProcessed);

        // Upisivanje svakog pojedinačnog obrađenog zapisa u MongoDB (odakle
        // Superset/Aggregations mogu lako izvući prosek)
        mergedStream.foreach((key, doc) -> {
            try {
                collection.insertOne(doc);
                System.out.println("Upisano u MongoDB [Query 8]: Platform=" + doc.getString("platform") +
                        ", Tip dana=" + doc.getString("day_type") + ", Views=" + doc.getLong("views"));
            } catch (Exception e) {
                System.err.println("Greška pri upisu u MongoDB: " + e.getMessage());
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            mongoClient.close();
        }));

        streams.start();
        System.out.println("Query 8 Processor Started Successfully.");
    }

    /**
     * Pomoćna metoda za određivanje da li je datum radni dan ili vikend.
     */
    private static String determineDayType(String timestampStr) {
        try {
            // Parsiramo ISO-8601 string u Instant, pa u LocalDate (UTC zona)
            Instant instant = Instant.parse(timestampStr);
            LocalDate date = instant.atZone(ZoneOffset.UTC).toLocalDate();
            DayOfWeek dayOfWeek = date.getDayOfWeek();

            // Subota (SATURDAY) i Nedelja (SUNDAY) su vikend, sve ostalo su radni dani
            if (dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY) {
                return "Weekend";
            } else {
                return "Weekday";
            }
        } catch (Exception e) {
            // Ako parsiranje iz nekog razloga ne uspe, tretiramo kao radni dan po default-u
            return "Weekday";
        }
    }
}