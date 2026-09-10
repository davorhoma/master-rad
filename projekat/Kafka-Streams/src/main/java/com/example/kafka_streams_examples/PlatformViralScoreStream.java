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

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Properties;

public class PlatformViralScoreStream {
    private static final Logger log = LoggerFactory.getLogger(PlatformViralScoreStream.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://admin:admin@localhost:27017");
        String mongoDbName = System.getenv().getOrDefault("MONGO_DB", "realtime_data");
        String mongoCollectionName = System.getenv().getOrDefault("MONGO_COLLECTION", "query_platform_viral_metrics");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-processor-platform-viral-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(mongoDbName);
        MongoCollection<Document> collection = database.getCollection(mongoCollectionName);

        StreamsBuilder builder = new StreamsBuilder();

        // Prate se samo search topik za YouTube i TikTok
        KStream<String, String> ytSearch = builder.stream("yt-search-topic");
        KStream<String, String> ttSearch = builder.stream("tt-search-topic");

        // Mapiranje YouTube unosa
        KStream<String, PlatformMetrics> ytMapped = ytSearch.map((key, value) -> parseMessage(value, "youtube"));
        // Mapiranje TikTok unosa
        KStream<String, PlatformMetrics> ttMapped = ttSearch.map((key, value) -> parseMessage(value, "tiktok"));

        // Spajanje i obrada oba streama
        KStream<String, PlatformMetrics> allMapped = ytMapped.merge(ttMapped)
                .filter((k, v) -> v != null && !k.equals("ERROR"));

        // Grupisanje po složenom ključu: tema_platforma (npr. "roblox_youtube")
        KTable<String, PlatformMetrics> aggregatedTable = allMapped
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(PlatformMetrics.class)))
                .reduce((aggValue, newValue) -> {
                    double newViralSum = aggValue.viralScoreSum + newValue.viralScoreSum;
                    long newCount = aggValue.count + newValue.count;
                    return new PlatformMetrics(
                            newValue.theme,
                            newValue.platform,
                            newValue.day,
                            newValue.epochDay,
                            newViralSum,
                            newCount);
                });

        aggregatedTable.toStream().foreach((key, value) -> {
            try {
                double avgViralScore = value.count > 0 ? value.viralScoreSum / value.count : 0.0;
                long currentEpochDay = LocalDate.now().toEpochDay();

                // Upis u MongoDB sa upsert opcijom po temi, platformi i danu
                Document doc = new Document("topic", value.theme)
                        .append("platform", value.platform)
                        .append("day", value.day)
                        .append("epoch_day", currentEpochDay)
                        .append("avg_viral_score", avgViralScore)
                        .append("total_records", value.count);

                collection.updateOne(
                        com.mongodb.client.model.Filters.and(
                                com.mongodb.client.model.Filters.eq("topic", value.theme),
                                com.mongodb.client.model.Filters.eq("platform", value.platform),
                                com.mongodb.client.model.Filters.eq("day", value.day)),
                        new Document("$set", doc),
                        new UpdateOptions().upsert(true));

                log.info("Platforma: {}, Tema: {}, Dan: {}, Prosečan viral score: {}",
                        value.platform, value.theme, value.day, avgViralScore);
            } catch (Exception e) {
                log.error("Greška pri upisu u MongoDB za temu: {}", value.theme, e);
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

    private static KeyValue<String, PlatformMetrics> parseMessage(String value, String platform) {
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
            long epochDay = LocalDate.now().toEpochDay();
            String timeField = node.has("publish_time") ? node.get("publish_time").asText()
                    : (node.has("published_at") ? node.get("published_at").asText()
                            : (node.has("trending_detected_time") ? node.get("trending_detected_time").asText()
                                    : null));

            if (timeField != null) {
                try {
                    OffsetDateTime odt = OffsetDateTime.parse(timeField);
                    day = odt.getDayOfMonth();
                    epochDay = odt.toLocalDate().toEpochDay();
                } catch (Exception ignored) {
                }
            }

            double viralScore = node.has("viral_score") ? node.get("viral_score").asDouble(0.0) : 0.0;

            String streamKey = canonicalTheme + "_" + platform;

            return KeyValue.pair(streamKey,
                    new PlatformMetrics(canonicalTheme, platform, day, epochDay, viralScore, 1));
        } catch (Exception e) {
            log.error("Greška pri parsiranju poruke sa platforme {}: {}", platform, value, e);
            return KeyValue.pair("ERROR", null);
        }
    }

    private static String mapToCanonicalTheme(String rawTag) {
        if (rawTag == null)
            return "other gaming";
        String tag = rawTag.toLowerCase().trim();

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

    public static class PlatformMetrics {
        public String theme;
        public String platform;
        public int day;
        public long epochDay;
        public double viralScoreSum;
        public long count;

        public PlatformMetrics() {
        }

        public PlatformMetrics(String theme, String platform, int day, long epochDay, double viralScoreSum,
                long count) {
            this.theme = theme;
            this.platform = platform;
            this.day = day;
            this.epochDay = epochDay;
            this.viralScoreSum = viralScoreSum;
            this.count = count;
        }
    }

    public static class JsonSerde<T> extends Serdes.WrapperSerde<T> {
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