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
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.bson.Document;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ViewsByThemeTimelineStream {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature());

    public static class DetailedViewsAggregation {
        public String platform;
        public String theme;
        public String timeWindow;
        public long totalViews;
        public long count;

        public DetailedViewsAggregation() {
            this.totalViews = 0L;
            this.count = 0L;
        }

        public DetailedViewsAggregation(String platform, String theme, String timeWindow, long totalViews, long count) {
            this.platform = platform;
            this.theme = theme;
            this.timeWindow = timeWindow;
            this.totalViews = totalViews;
            this.count = count;
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
        System.out.println("Started Views By Gaming Theme Timeline Processor with Payload Timestamps.");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "views-by-theme-timeline-processor-v1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092"));
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://admin:admin@mongodb:27017");
        String mongoDbName = System.getenv().getOrDefault("MONGO_DB", "realtime_data");
        String mongoCollectionName = System.getenv().getOrDefault("MONGO_COLLECTION", "query_2_views_timeline");

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(mongoDbName);
        MongoCollection<Document> collection = database.getCollection(mongoCollectionName);

        StreamsBuilder builder = new StreamsBuilder();

        // Brojači za ulazne poruke i upise u bazu
        AtomicLong ytMessageCounter = new AtomicLong(0);
        AtomicLong ttMessageCounter = new AtomicLong(0);
        AtomicLong mongoWriteCounter = new AtomicLong(0);

        KStream<String, String> ytStream = builder.stream("yt-monitoring-topic");
        KStream<String, String> ttStream = builder.stream("tt-monitoring-topic");

        KStream<String, Document> ytProcessed = ytStream.flatMap((key, value) -> {
            List<KeyValue<String, Document>> result = new ArrayList<>();
            try {
                JsonNode node = objectMapper.readTree(value);
                long views = parseViews(node);
                String timeWindow = extractDate(node, "YouTube");

                String tagsStr = node.has("tags") ? node.path("tags").asText("") : "";
                if (!tagsStr.isEmpty()) {
                    String[] tags = tagsStr.split("\\|");
                    for (String tag : tags) {
                        String rawTheme = tag.trim().toLowerCase();
                        if (!rawTheme.isEmpty()) {
                            Document doc = new Document("platform", "YouTube")
                                    .append("theme", rawTheme)
                                    .append("time_window", timeWindow)
                                    .append("views", views);
                            result.add(new KeyValue<>("YouTube_" + rawTheme + "_" + timeWindow, doc));
                        }
                    }
                }
                ytMessageCounter.incrementAndGet();
            } catch (Exception e) {
                System.err.println("Greška pri mapiranju YT JSON-a: " + e.getMessage());
            }
            return result;
        });

        KStream<String, Document> ttProcessed = ttStream.flatMap((key, value) -> {
            List<KeyValue<String, Document>> result = new ArrayList<>();
            try {
                JsonNode node = objectMapper.readTree(value);
                long views = parseViews(node);
                String timeWindow = extractDate(node, "TikTok");

                String hashtagsStr = node.has("hashtags") ? node.path("hashtags").asText("") : "";
                List<String> hashtagNames = extractTikTokHashtagNames(hashtagsStr);

                for (String hashtag : hashtagNames) {
                    String rawTheme = hashtag.trim().toLowerCase();
                    if (!rawTheme.isEmpty()) {
                        Document doc = new Document("platform", "TikTok")
                                .append("theme", rawTheme)
                                .append("time_window", timeWindow)
                                .append("views", views);
                        result.add(new KeyValue<>("TikTok_" + rawTheme + "_" + timeWindow, doc));
                    }
                }
                ttMessageCounter.incrementAndGet();
            } catch (Exception e) {
                System.err.println("Greška pri mapiranju TT JSON-a: " + e.getMessage());
            }
            return result;
        });

        KStream<String, Document> mergedStream = ytProcessed.merge(ttProcessed);

        // Agregacija po platformi, kanonskoj temi i datumu iz payload-a
        KTable<String, DetailedViewsAggregation> aggregatedThemesTimeline = mergedStream
                .map((key, doc) -> {
                    String platform = doc.getString("platform");
                    String rawTheme = doc.getString("theme");
                    String timeWindow = doc.getString("time_window");
                    String canonicalTheme = mapToCanonicalTheme(rawTheme);
                    long views = doc.getLong("views");

                    String groupKey = platform + "_" + canonicalTheme + "_" + timeWindow;
                    DetailedViewsAggregation initialVal = new DetailedViewsAggregation(platform, canonicalTheme,
                            timeWindow, views, 1L);
                    return new KeyValue<>(groupKey, initialVal);
                })
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(DetailedViewsAggregation.class)))
                .aggregate(
                        DetailedViewsAggregation::new,
                        (aggKey, newRecord, aggregate) -> {
                            if (aggregate.platform == null)
                                aggregate.platform = newRecord.platform;
                            if (aggregate.theme == null)
                                aggregate.theme = newRecord.theme;
                            if (aggregate.timeWindow == null)
                                aggregate.timeWindow = newRecord.timeWindow;
                            aggregate.totalViews += newRecord.totalViews;
                            aggregate.count += newRecord.count;
                            return aggregate;
                        },
                        Materialized.<String, DetailedViewsAggregation, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(
                                "views-timeline-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(DetailedViewsAggregation.class)));

        // Upis u MongoDB
        aggregatedThemesTimeline.toStream().foreach((aggKey, aggregation) -> {
            try {
                if (aggregation == null || aggregation.theme == null || aggregation.timeWindow == null)
                    return;

                double averageViews = aggregation.count > 0 ? (double) aggregation.totalViews / aggregation.count : 0.0;
                double roundedAvg = Math.round(averageViews * 100.0) / 100.0;

                Document mongoDoc = new Document("platform", aggregation.platform)
                        .append("theme", aggregation.theme)
                        .append("time_window", aggregation.timeWindow)
                        .append("total_views", aggregation.totalViews)
                        .append("average_views", roundedAvg)
                        .append("count", aggregation.count);

                collection.findOneAndReplace(
                        Filters.and(
                                Filters.eq("platform", aggregation.platform),
                                Filters.eq("theme", aggregation.theme),
                                Filters.eq("time_window", aggregation.timeWindow)),
                        mongoDoc,
                        new FindOneAndReplaceOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

                long totalWrites = mongoWriteCounter.incrementAndGet();
                long totalYt = ytMessageCounter.get();
                long totalTt = ttMessageCounter.get();

                if (totalWrites % 1000 == 0 || (totalYt + totalTt) % 1000 == 0) {
                    System.out.println(
                            "Pročitano poruka -> YT: " + totalYt +
                                    " | TT: " + totalTt +
                                    " | Upisano u MongoDB: " + totalWrites +
                                    " | Poslednji prozor: Platform=" + aggregation.platform +
                                    ", Theme=" + aggregation.theme +
                                    ", Window=" + aggregation.timeWindow +
                                    ", Total Views=" + aggregation.totalViews);
                }

            } catch (Exception e) {
                System.err.println("KRITIČNA GREŠKA U MONGODB UPSERT-u za ključ: " + aggKey);
                e.printStackTrace();
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            mongoClient.close();
        }));

        streams.start();
        System.out.println("Views By Gaming Theme Timeline Processor Started Successfully.");
    }

    private static String extractDate(JsonNode node, String platform) {
        String fieldName = platform.equals("YouTube") ? "collected_at" : "trending_detected_time";
        if (node.has(fieldName)) {
            String timeStr = node.path(fieldName).asText("");
            if (timeStr.length() >= 10) {
                return timeStr.substring(0, 10);
            }
        }
        return LocalDate.now(ZoneId.of("UTC")).toString();
    }

    private static long parseViews(JsonNode node) {
        if (node.has("views")) {
            JsonNode v = node.path("views");
            if (v.isNumber())
                return v.asLong(0L);
            if (v.isTextual()) {
                try {
                    return Long.parseLong(v.asText());
                } catch (Exception ignored) {
                }
            }
        }
        if (node.has("view_count")) {
            JsonNode v = node.path("view_count");
            if (v.isNumber())
                return v.asLong(0L);
            if (v.isTextual()) {
                try {
                    return Long.parseLong(v.asText());
                } catch (Exception ignored) {
                }
            }
        }
        if (node.has("play_count")) {
            JsonNode v = node.path("play_count");
            if (v.isNumber())
                return v.asLong(0L);
            if (v.isTextual()) {
                try {
                    return Long.parseLong(v.asText());
                } catch (Exception ignored) {
                }
            }
        }
        return 0L;
    }

    private static List<String> extractTikTokHashtagNames(String hashtagsStr) {
        List<String> names = new ArrayList<>();
        if (hashtagsStr == null || hashtagsStr.isEmpty()) {
            return names;
        }

        Pattern pattern = Pattern.compile("'name':\\s*'([^']+)'|\"name\":\\s*\"([^\"]+)\"");
        Matcher matcher = pattern.matcher(hashtagsStr);

        while (matcher.find()) {
            String name = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
            if (name != null && !name.isEmpty()) {
                names.add(name);
            }
        }

        if (names.isEmpty() && hashtagsStr.trim().startsWith("[")) {
            try {
                JsonNode arrayNode = objectMapper.readTree(hashtagsStr);
                if (arrayNode.isArray()) {
                    for (JsonNode item : arrayNode) {
                        if (item.has("name")) {
                            names.add(item.path("name").asText());
                        }
                    }
                }
            } catch (Exception ignored) {
            }
        }

        return names;
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
}