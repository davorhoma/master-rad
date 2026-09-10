package com.example.kafka_streams_examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.Properties;
import static com.mongodb.client.model.Filters.*;

public class EmergingTopicsStream {
    private static final Logger log = LoggerFactory.getLogger(EmergingTopicsStream.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://admin:admin@localhost:27017");
        String mongoDbName = System.getenv().getOrDefault("MONGO_DB", "realtime_data");
        String mongoCollectionName = System.getenv().getOrDefault("MONGO_COLLECTION",
                "query_9_emerging_topics_metrics");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-processor-q9-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(mongoDbName);
        MongoCollection<Document> collection = database.getCollection(mongoCollectionName);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> ytSearch = builder.stream("yt-search-topic");
        KStream<String, String> ttSearch = builder.stream("tt-search-topic");
        KStream<String, String> ytMonitoring = builder.stream("yt-monitoring-topic");
        KStream<String, String> ttMonitoring = builder.stream("tt-monitoring-topic");

        KStream<String, String> allStreams = ytSearch.merge(ttSearch).merge(ytMonitoring).merge(ttMonitoring);

        KStream<String, SearchMetrics> mappedStream = allStreams.map((key, value) -> {
            try {
                JsonNode node = objectMapper.readTree(value);

                String rawTag = "";
                if (node.has("tags") && !node.get("tags").isNull()) {
                    rawTag = node.get("tags").asText();
                } else if (node.has("hashtags") && !node.get("hashtags").isNull()) {
                    rawTag = node.get("hashtags").asText();
                } else if (node.has("description") && !node.get("description").isNull()) {
                    rawTag = node.get("description").asText();
                } else if (node.has("title") && !node.get("title").isNull()) {
                    rawTag = node.get("title").asText();
                }

                String canonicalTheme = mapToCanonicalTheme(rawTag);

                int day = 1;
                String timeField = node.has("publish_time") ? node.get("publish_time").asText()
                        : (node.has("published_at") ? node.get("published_at").asText()
                                : (node.has("trending_detected_time") ? node.get("trending_detected_time").asText()
                                        : null));

                if (timeField != null) {
                    try {
                        OffsetDateTime odt = OffsetDateTime.parse(timeField);
                        day = odt.getDayOfMonth();
                    } catch (Exception ignored) {
                    }
                }

                double viralScore = node.has("viral_score") ? node.get("viral_score").asDouble(0.0) : 0.0;

                // Ekstrakcija pregleda (podržava view_count, views, play_count...)
                long views = 0;
                if (node.has("view_count") && !node.get("view_count").isNull()) {
                    views = node.get("view_count").asLong(0L);
                } else if (node.has("views") && !node.get("views").isNull()) {
                    views = node.get("views").asLong(0L);
                } else if (node.has("play_count") && !node.get("play_count").isNull()) {
                    views = node.get("play_count").asLong(0L);
                }

                boolean isSearch = true;

                return KeyValue.pair(canonicalTheme, //+ "_" + day,
                        new SearchMetrics(canonicalTheme, day, viralScore, views, 1, isSearch));
            } catch (Exception e) {
                log.error("Greška pri parsiranju poruke: {}", value, e);
                return KeyValue.pair("ERROR", null);
            }
        }).filter((k, v) -> v != null && !k.equals("ERROR"));

        KTable<String, SearchMetrics> aggregatedTable = mappedStream
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(SearchMetrics.class)))
                .reduce((aggValue, newValue) -> {
                    double newViralSum = aggValue.viralScoreSum + newValue.viralScoreSum;
                    long newViewsSum = aggValue.viewsSum + newValue.viewsSum;
                    long newCount = aggValue.count + newValue.count;
                    return new SearchMetrics(newValue.theme, newValue.day, newViralSum, newViewsSum, newCount,
                            newValue.isSearch);
                });

        aggregatedTable.toStream().foreach((key, value) -> {
            try {
                double avgViralScore = value.count > 0 ? value.viralScoreSum / value.count : 0.0;
                double avgViews = value.count > 0 ? (double) value.viewsSum / value.count : 0.0;

                long nowEpochDay = java.time.LocalDate.now().toEpochDay();
                long threeDaysAgoEpoch = nowEpochDay - 3;

                double viralGrowthRate = 0.0;
                double viewsGrowthRate = 0.0;

                org.bson.conversions.Bson queryFilter = com.mongodb.client.model.Filters.and(
                        com.mongodb.client.model.Filters.eq("topic", value.theme),
                        com.mongodb.client.model.Filters.gte("epoch_day", threeDaysAgoEpoch),
                        com.mongodb.client.model.Filters.lt("epoch_day", nowEpochDay));

                double sumPastViralScores = 0.0;
                double sumPastViews = 0.0;
                int pastCount = 0;

                try (com.mongodb.client.MongoCursor<Document> cursor = collection.find(queryFilter).iterator()) {
                    while (cursor.hasNext()) {
                        Document pastDoc = cursor.next();
                        if (pastDoc.containsKey("avg_viral_score") && pastDoc.containsKey("avg_views")) {
                            sumPastViralScores += pastDoc.getDouble("avg_viral_score");
                            sumPastViews += pastDoc.getDouble("avg_views");
                            pastCount++;
                        }
                    }
                }

                if (pastCount > 0) {
                    double avgPastViralScore = sumPastViralScores / pastCount;
                    if (avgPastViralScore > 0.0) {
                        viralGrowthRate = ((avgViralScore - avgPastViralScore) / avgPastViralScore) * 100.0;
                    }

                    double avgPastViews = sumPastViews / pastCount;
                    if (avgPastViews > 0.0) {
                        viewsGrowthRate = ((avgViews - avgPastViews) / avgPastViews) * 100.0;
                    }
                }

                Document doc = new Document("topic", value.theme)
                        .append("day", value.day)
                        .append("epoch_day", nowEpochDay)
                        .append("avg_viral_score", avgViralScore)
                        .append("avg_views", avgViews)
                        .append("3-day viral score growth rate", viralGrowthRate)
                        .append("3-day views growth rate", viewsGrowthRate)
                        .append("is_search", value.isSearch);

                collection.updateOne(
                        com.mongodb.client.model.Filters.and(
                                com.mongodb.client.model.Filters.eq("topic", value.theme),
                                com.mongodb.client.model.Filters.eq("day", value.day)),
                        new Document("$set", doc),
                        new UpdateOptions().upsert(true));

                log.info("Tema: {}, Rast viralnosti: {}%, Rast pregleda: {}%, Trenutni skor: {}, Trenutni pregledi: {}",
                        value.theme, viralGrowthRate, viewsGrowthRate, avgViralScore, avgViews);
            } catch (Exception e) {
                log.error("Greška pri izračunavanju rasta i upisu u MongoDB", e);
            }
        });

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            mongoClient.close();
        }));

        streams.start();
    }

    // Vaša funkcija za mapiranje tagova/opisa u kanonske teme
    private static String mapToCanonicalTheme(String rawTag) {
        if (rawTag == null)
            return "other gaming";
        String tag = rawTag.toLowerCase().trim();

        // Popular Specific Games
        if (tag.contains("roblox"))
            return "roblox";
        if (tag.contains("minecraft"))
            return "minecraft";
        if (tag.contains("cs2") || tag.contains("counter-strike") || tag.contains("counter strike"))
            return "counter-strike 2";
        if (tag.contains("valorant"))
            return "valorant";
        if (tag.contains("fortnite"))
            return "fortnite";
        if (tag.contains("gta") || tag.contains("grand theft auto"))
            return "gta";
        if (tag.contains("league of legends") || tag.equals("lol"))
            return "league of legends";
        if (tag.contains("dota"))
            return "dota 2";
        if (tag.contains("apex"))
            return "apex legends";
        if (tag.contains("overwatch"))
            return "overwatch 2";
        if (tag.contains("call of duty") || tag.contains("warzone") || tag.contains("cod"))
            return "call of duty";
        if (tag.contains("pubg"))
            return "pubg";
        if (tag.contains("free fire"))
            return "free fire";
        if (tag.contains("brawl stars"))
            return "brawl stars";
        if (tag.contains("clash royale"))
            return "clash royale";
        if (tag.contains("clash of clans"))
            return "clash of clans";
        if (tag.contains("genshin"))
            return "genshin impact";
        if (tag.contains("fifa") || tag.contains("fc 25") || tag.contains("ea sports fc"))
            return "sports / fifa";
        if (tag.contains("nba 2k"))
            return "sports / nba 2k";
        if (tag.contains("rocket league"))
            return "rocket league";
        if (tag.contains("rainbow six") || tag.contains("r6"))
            return "rainbow six siege";
        if (tag.contains("elden ring"))
            return "elden ring";
        if (tag.contains("dark souls"))
            return "dark souls";
        if (tag.contains("wukong") || tag.contains("black myth"))
            return "black myth wukong";
        if (tag.contains("cyberpunk"))
            return "cyberpunk 2077";
        if (tag.contains("red dead") || tag.contains("rdr2"))
            return "red dead redemption 2";
        if (tag.contains("skyrim") || tag.contains("elder scrolls"))
            return "skyrim";
        if (tag.contains("resident evil"))
            return "resident evil";
        if (tag.contains("fnaf") || tag.contains("five nights"))
            return "five nights at freddys";
        if (tag.contains("phasmophobia"))
            return "phasmophobia";
        if (tag.contains("lethal company"))
            return "lethal company";
        if (tag.contains("among us"))
            return "among us";
        if (tag.contains("helldivers"))
            return "helldivers 2";
        if (tag.contains("monster hunter"))
            return "monster hunter wilds";
        if (tag.contains("mario kart"))
            return "mario kart";
        if (tag.contains("zelda"))
            return "zelda";
        if (tag.contains("pokemon"))
            return "pokemon";

        // Categories, Genres & Formats
        if (tag.contains("esports") || tag.contains("e-sports") || tag.contains("competitive")
                || tag.contains("pro gamer"))
            return "esports";
        if (tag.contains("survival"))
            return "survival";
        if (tag.contains("horror") || tag.contains("scp"))
            return "horror";
        if (tag.contains("battle royale"))
            return "battle royale";
        if (tag.contains("rpg") || tag.contains("roleplay") || tag.contains("role-play"))
            return "rpg";
        if (tag.contains("strategy") || tag.contains("tactic"))
            return "strategy game";
        if (tag.contains("simulator") || tag.contains("simulation"))
            return "simulator game";
        if (tag.contains("indie"))
            return "indie game";
        if (tag.contains("open world") || tag.contains("sandbox"))
            return "open world / sandbox";
        if (tag.contains("anime"))
            return "anime games";
        if (tag.contains("retro"))
            return "retro gaming";
        if (tag.contains("mobile"))
            return "mobile gaming";
        if (tag.contains("vr") || tag.contains("virtual reality"))
            return "vr gaming";

        // Content Types / Video Styles
        if (tag.contains("walkthrough") || tag.contains("playthrough") || tag.contains("no commentary"))
            return "walkthrough / playthrough";
        if (tag.contains("speedrun"))
            return "speedrun";
        if (tag.contains("boss fight"))
            return "boss fight";
        if (tag.contains("tutorial") || tag.contains("tips") || tag.contains("tricks") || tag.contains("guide")
                || tag.contains("settings"))
            return "tutorial / guides";
        if (tag.contains("highlight") || tag.contains("montage") || tag.contains("clip") || tag.contains("short")
                || tag.contains("top plays") || tag.contains("clutch") || tag.contains("trickshot"))
            return "highlights & clips";
        if (tag.contains("funny") || tag.contains("meme") || tag.contains("fail") || tag.contains("rage")
                || tag.contains("reaction"))
            return "funny moments & memes";
        if (tag.contains("stream") || tag.contains("twitch") || tag.contains("livestream"))
            return "livestream / vod";
        if (tag.contains("review") || tag.contains("news"))
            return "gaming news & reviews";
        if (tag.contains("challenge"))
            return "gaming challenges";
        if (tag.contains("co-op") || tag.contains("multiplayer"))
            return "multiplayer / co-op";

        // Hardware & Setup
        if (tag.contains("setup") || tag.contains("keyboard") || tag.contains("mouse") || tag.contains("hardware")
                || tag.contains("aim training"))
            return "hardware & setup";
        if (tag.contains("ai gaming") || tag.contains("unreal engine"))
            return "tech & ai gaming";

        if (tag.contains("fps") || tag.contains("firstperson") || tag.contains("first-person")
                || tag.contains("shooter"))
            return "fps";

        return "other gaming";
    }

    public static class SearchMetrics {
        public String theme;
        public int day;
        public double viralScoreSum;
        public long viewsSum;
        public long count;
        public boolean isSearch;

        public SearchMetrics() {
        }

        public SearchMetrics(String theme, int day, double viralScoreSum, long viewsSum, long count, boolean isSearch) {
            this.theme = theme;
            this.day = day;
            this.viralScoreSum = viralScoreSum;
            this.viewsSum = viewsSum;
            this.count = count;
            this.isSearch = isSearch;
        }
    }

    public static class JsonSerde<T> extends Serdes.WrapperSerde<T> {
        @SuppressWarnings("unchecked")
        public JsonSerde(Class<T> type) {
            super(new JsonSerializer<>(), new JsonDeserializer<>(type));
        }
    }

    public static class JsonSerializer<T> implements org.apache.kafka.common.serialization.Serializer<T> {
        @Override
        public byte[] serialize(String topic, T data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Greška pri serijalizaciji", e);
            }
        }
    }

    public static class JsonDeserializer<T> implements org.apache.kafka.common.serialization.Deserializer<T> {
        private final Class<T> targetType;

        public JsonDeserializer(Class<T> targetType) {
            this.targetType = targetType;
        }

        @Override
        public T deserialize(String topic, byte[] bytes) {
            if (bytes == null)
                return null;
            try {
                return objectMapper.readValue(bytes, targetType);
            } catch (Exception e) {
                throw new RuntimeException("Greška pri deserijalizaciji", e);
            }
        }
    }
}