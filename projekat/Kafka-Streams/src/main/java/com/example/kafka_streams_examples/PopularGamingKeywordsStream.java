package com.example.kafka_streams_examples;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.bson.Document;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

public class PopularGamingKeywordsStream {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature());

    // Brojači za praćenje pročitanih poruka sa topika u realnom vremenu
    private static final AtomicLong ytMessageCount = new AtomicLong(0);
    private static final AtomicLong ttMessageCount = new AtomicLong(0);

    // Osnovne stop reči (engleske i srpske/lokalne) koje želimo da ignorišemo
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            "the", "and", "to", "a", "of", "in", "for", "is", "on", "that", "by", "this", "with",
            "i", "you", "it", "not", "or", "be", "are", "from", "at", "as", "your", "all", "have",
            "new", "more", "an", "was", "we", "will", "my", "one", "all", "would", "there", "their",
            "i", "je", "se", "da", "su", "u", "na", "za", "sa", "po", "od", "do", "kako", "što",
            // Dodati domeni i URL fragmenti
            "https", "http", "www", "com", "net", "org", "br",
            // Dodate društvene mreže i često korišćene reči iz opisa
            "youtube", "instagram", "twitch", "discord", "tiktok", "twitter", "kick", "youtu",
            "facebook", "gmail", "channel", "canal", "video", "videos", "link", "email", "contato"));

    public static void main(String[] args) {
        System.out.println("Started Popular Gaming Keywords Processor.");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "popular-gaming-keywords-processor-v1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092"));
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // MongoDB konekcija
        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://admin:admin@mongodb:27017");
        String mongoDbName = System.getenv().getOrDefault("MONGO_DB", "realtime_data");
        String mongoCollectionName = System.getenv().getOrDefault("MONGO_COLLECTION", "query_3_popular_keywords");

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(mongoDbName);
        MongoCollection<Document> collection = database.getCollection(mongoCollectionName);

        StreamsBuilder builder = new StreamsBuilder();

        // Naziv State Store-a za praćenje frekvencije reči po platformama
        String storeName = "gaming-keywords-store";
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(storeName),
                        Serdes.String(),
                        Serdes.String()));

        // Čitanje sa YouTube i TikTok topika
        KStream<String, String> ytStream = builder.stream("yt-monitoring-topic");
        KStream<String, String> ttStream = builder.stream("tt-monitoring-topic");

        // Mapiranje YouTube poruka uz inkrementiranje i povremeni ispis brojača
        KStream<String, Document> ytProcessed = ytStream.peek((key, value) -> {
            long currentCount = ytMessageCount.incrementAndGet();
            if (currentCount % 500 == 0 || currentCount == 10467) {
                System.out.println("[YouTube Progress] Pročitano poruka: " + currentCount + " / 10467");
            }
        }).flatMap((key, value) -> {
            java.util.List<KeyValue<String, Document>> result = new java.util.ArrayList<>();
            try {
                JsonNode node = objectMapper.readTree(value);
                String title = node.has("title") ? node.path("title").asText("") : "";
                String description = node.has("description") ? node.path("description").asText("") : "";

                String combinedText = (title + " " + description).toLowerCase();
                java.util.List<String> keywords = extractKeywords(combinedText);

                for (String keyword : keywords) {
                    Document doc = new Document("platform", "YouTube")
                            .append("keyword", keyword);
                    result.add(new KeyValue<>("YouTube_" + keyword, doc));
                }
            } catch (Exception e) {
                System.err.println("Greška pri mapiranju YT JSON-a: " + e.getMessage());
            }
            return result;
        });

        // Mapiranje TikTok poruka uz inkrementiranje i povremeni ispis brojača
        KStream<String, Document> ttProcessed = ttStream.peek((key, value) -> {
            long currentCount = ttMessageCount.incrementAndGet();
            if (currentCount % 2000 == 0 || currentCount == 43338) {
                System.out.println("[TikTok Progress] Pročitano poruka: " + currentCount + " / 43338");
            }
        }).flatMap((key, value) -> {
            java.util.List<KeyValue<String, Document>> result = new java.util.ArrayList<>();
            try {
                JsonNode node = objectMapper.readTree(value);
                String desc = "";
                if (node.has("description")) {
                    desc = node.path("description").asText("");
                } else if (node.has("desc")) {
                    desc = node.path("desc").asText("");
                }

                String combinedText = desc.toLowerCase();
                java.util.List<String> keywords = extractKeywords(combinedText);

                for (String keyword : keywords) {
                    Document doc = new Document("platform", "TikTok")
                            .append("keyword", keyword);
                    result.add(new KeyValue<>("TikTok_" + keyword, doc));
                }
            } catch (Exception e) {
                System.err.println("Greška pri mapiranju TT JSON-a: " + e.getMessage());
            }
            return result;
        });

        // Spajanje tokova
        KStream<String, Document> mergedStream = ytProcessed.merge(ttProcessed);

        // Korišćenje Processor API-ja sa baferovanjem i Punctuator-om za batch upis
        mergedStream.process(new ProcessorSupplier<String, Document, Void, Void>() {
            @Override
            public Processor<String, Document, Void, Void> get() {
                return new Processor<String, Document, Void, Void>() {
                    private KeyValueStore<String, String> stateStore;
                    private org.apache.kafka.streams.processor.api.ProcessorContext<Void, Void> context;

                    // Bafer za čuvanje stanja reči pre masovnog upisa u bazu (Ključ: storeKey,
                    // Vrednost: Document)
                    private final Map<String, Document> mongoBuffer = new HashMap<>();

                    @Override
                    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<Void, Void> context) {
                        this.context = context;
                        this.stateStore = context.getStateStore(storeName);

                        // Postavljanje Punctuator-a da se izvršava na svake 3 sekunde (Wall-clock time)
                        context.schedule(Duration.ofSeconds(3), PunctuationType.WALL_CLOCK_TIME,
                                timestamp -> flushBatchToMongo());
                    }

                    @Override
                    public void process(Record<String, Document> record) {
                        try {
                            String storeKey = record.key();
                            Document value = record.value();
                            if (storeKey == null || value == null)
                                return;

                            String platform = value.getString("platform");
                            String keyword = value.getString("keyword");

                            // Učitavanje trenutnog stanja iz State Store-a
                            String existingDataJson = stateStore.get(storeKey);
                            long count = 1;

                            if (existingDataJson != null) {
                                Document existingDoc = Document.parse(existingDataJson);
                                Number existingCount = existingDoc.get("count", Number.class);
                                count = (existingCount != null ? existingCount.longValue() : 0) + 1;
                            }

                            // Ažuriranje lokalnog Kafka Streams State Store-a
                            Document newStateDoc = new Document("platform", platform)
                                    .append("keyword", keyword)
                                    .append("count", count);
                            stateStore.put(storeKey, newStateDoc.toJson());

                            // Dodavanje u lokalni bafer za kasniji batch upis u MongoDB
                            synchronized (mongoBuffer) {
                                mongoBuffer.put(storeKey, newStateDoc);
                            }

                        } catch (Exception e) {
                            System.err.println("KRITIČNA GREŠKA U PROCESORU za ključ: " + record.key());
                            e.printStackTrace();
                        }
                    }

                    // Metoda koja se poziva na svake 3 sekunde i prazni bafer u MongoDB jednim bulk
                    // upisom
                    private void flushBatchToMongo() {
                        Map<String, Document> batchCopy;
                        synchronized (mongoBuffer) {
                            if (mongoBuffer.isEmpty()) {
                                return;
                            }
                            batchCopy = new HashMap<>(mongoBuffer);
                            mongoBuffer.clear();
                        }

                        try {
                            java.util.List<WriteModel<Document>> writes = new java.util.ArrayList<>();
                            for (Document doc : batchCopy.values()) {
                                String platform = doc.getString("platform");
                                String keyword = doc.getString("keyword");

                                org.bson.conversions.Bson filter = com.mongodb.client.model.Filters.and(
                                        com.mongodb.client.model.Filters.eq("platform", platform),
                                        com.mongodb.client.model.Filters.eq("keyword", keyword));

                                writes.add(
                                        new ReplaceOneModel<Document>(filter, doc, new ReplaceOptions().upsert(true)));
                            }

                            long startTime = System.currentTimeMillis();
                            if (!writes.isEmpty()) {
                                collection.bulkWrite(writes);
                                long duration = System.currentTimeMillis() - startTime;
                                System.out.println("[MongoDB Batch] Uspešno upisano " + writes.size()
                                        + " jedinstvenih ključnih reči za " + duration + " ms.");
                            }
                        } catch (Exception e) {
                            System.err.println("Greška pri batch upisu u MongoDB: " + e.getMessage());
                        }
                    }

                    @Override
                    public void close() {
                        // Osiguravamo da se pre gašenja aplikacije upiše preostali sadržaj iz bafera
                        flushBatchToMongo();
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
        System.out.println("Popular Gaming Keywords Processor Started Successfully.");
    }

    private static java.util.List<String> extractKeywords(String text) {
        java.util.List<String> validWords = new java.util.ArrayList<>();
        if (text == null || text.isEmpty()) {
            return validWords;
        }

        String cleanedText = text.replaceAll("[^a-zA-Z0-9čćžšđČĆŽŠĐ]", " ");
        String[] tokens = cleanedText.split("\\s+");

        for (String token : tokens) {
            token = token.trim();
            if (token.length() > 2 && !STOP_WORDS.contains(token) && !token.matches("\\d+")) {
                validWords.add(token);
            }
        }

        return validWords;
    }
}