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
import org.apache.kafka.streams.state.KeyValueStore;
import org.bson.Document;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TrendDelayBetweenPlatformsStream {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature());

    public static class ThemePlatformState {
        public String theme;
        public String ytFirstDate;
        public String ttFirstDate;

        public ThemePlatformState() {
        }

        public ThemePlatformState(String theme, String ytFirstDate, String ttFirstDate) {
            this.theme = theme;
            this.ytFirstDate = ytFirstDate;
            this.ttFirstDate = ttFirstDate;
        }
    }

    public static class PlatformDateRecord {
        public String platform;
        public String theme;
        public String date;

        public PlatformDateRecord() {
        }

        public PlatformDateRecord(String platform, String theme, String date) {
            this.platform = platform;
            this.theme = theme;
            this.date = date;
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
        System.out.println("Started Trend Delay Between Platforms Processor.");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "trend-delay-platforms-processor-v1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092"));
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://admin:admin@mongodb:27017");
        String mongoDbName = System.getenv().getOrDefault("MONGO_DB", "realtime_data");
        String mongoCollectionName = System.getenv().getOrDefault("MONGO_COLLECTION", "query_6_trend_delay");

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(mongoDbName);
        MongoCollection<Document> collection = database.getCollection(mongoCollectionName);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> ytStream = builder.stream("yt-monitoring-topic");
        KStream<String, String> ttStream = builder.stream("tt-monitoring-topic");

        // Mapiranje YouTube unosa u [theme, PlatformDateRecord]
        KStream<String, PlatformDateRecord> ytProcessed = ytStream.flatMap((key, value) -> {
            List<KeyValue<String, PlatformDateRecord>> result = new ArrayList<>();
            try {
                JsonNode node = objectMapper.readTree(value);
                String tagsStr = node.has("tags") ? node.path("tags").asText("") : "";
                String timestampStr = node.has("collected_at") ? node.path("collected_at").asText()
                        : Instant.now().toString();
                LocalDate date = OffsetDateTime.parse(timestampStr).toLocalDate();

                if (!tagsStr.isEmpty()) {
                    for (String tag : tagsStr.split("\\|")) {
                        String rawTheme = tag.trim().toLowerCase();
                        if (!rawTheme.isEmpty()) {
                            String canonicalTheme = mapToCanonicalTheme(rawTheme);
                            PlatformDateRecord record = new PlatformDateRecord("YouTube", canonicalTheme,
                                    date.toString());
                            result.add(new KeyValue<>(canonicalTheme, record));
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Greška pri mapiranju YT JSON-a: " + e.getMessage());
            }
            return result;
        });

        // Mapiranje TikTok unosa u [theme, PlatformDateRecord]
        KStream<String, PlatformDateRecord> ttProcessed = ttStream.flatMap((key, value) -> {
            List<KeyValue<String, PlatformDateRecord>> result = new ArrayList<>();
            try {
                JsonNode node = objectMapper.readTree(value);
                String hashtagsStr = node.has("hashtags") ? node.path("hashtags").asText("") : "";
                String timestampStr = node.has("trending_detected_time") ? node.path("trending_detected_time").asText()
                        : (node.has("collected_at") ? node.path("collected_at").asText() : Instant.now().toString());
                LocalDate date = OffsetDateTime.parse(timestampStr).toLocalDate();

                for (String hashtag : extractTikTokHashtagNames(hashtagsStr)) {
                    String rawTheme = hashtag.trim().toLowerCase();
                    if (!rawTheme.isEmpty()) {
                        String canonicalTheme = mapToCanonicalTheme(rawTheme);
                        PlatformDateRecord record = new PlatformDateRecord("TikTok", canonicalTheme, date.toString());
                        result.add(new KeyValue<>(canonicalTheme, record));
                    }
                }
            } catch (Exception e) {
                System.err.println("Greška pri mapiranju TT JSON-a: " + e.getMessage());
            }
            return result;
        });

        // Spajanje tokova
        KStream<String, PlatformDateRecord> mergedStream = ytProcessed.merge(ttProcessed);

        // Agregacija po ključu (theme) sa ispravnim tipovima
        KTable<String, ThemePlatformState> aggregatedDelay = mergedStream
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(PlatformDateRecord.class)))
                .aggregate(
                        ThemePlatformState::new,
                        (aggKey, newRecord, aggregate) -> {
                            if (aggregate.theme == null)
                                aggregate.theme = aggKey;

                            if ("YouTube".equals(newRecord.platform)) {
                                if (aggregate.ytFirstDate == null
                                        || newRecord.date.compareTo(aggregate.ytFirstDate) < 0) {
                                    aggregate.ytFirstDate = newRecord.date;
                                }
                            } else if ("TikTok".equals(newRecord.platform)) {
                                if (aggregate.ttFirstDate == null
                                        || newRecord.date.compareTo(aggregate.ttFirstDate) < 0) {
                                    aggregate.ttFirstDate = newRecord.date;
                                }
                            }
                            return aggregate;
                        },
                        Materialized.<String, ThemePlatformState, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(
                                "trend-delay-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(ThemePlatformState.class)));

        // Sinhronizacija u MongoDB
        aggregatedDelay.toStream().foreach((theme, state) -> {
            try {
                if (state == null || state.ytFirstDate == null || state.ttFirstDate == null) {
                    // Potrebno je da tema postoji na obe platforme da bi se izračunalo kašnjenje
                    return;
                }

                LocalDate ytDate = LocalDate.parse(state.ytFirstDate);
                LocalDate ttDate = LocalDate.parse(state.ttFirstDate);

                String firstPlatform;
                long delayDays;

                if (ytDate.isBefore(ttDate)) {
                    firstPlatform = "YouTube";
                    delayDays = ChronoUnit.DAYS.between(ytDate, ttDate);
                } else if (ttDate.isBefore(ytDate)) {
                    firstPlatform = "TikTok";
                    delayDays = ChronoUnit.DAYS.between(ttDate, ytDate);
                } else {
                    firstPlatform = "Both / Simultaneous";
                    delayDays = 0L;
                }

                Document mongoDoc = new Document("theme", theme)
                        .append("first_platform", firstPlatform)
                        .append("delay_days", delayDays)
                        .append("youtube_first_date", state.ytFirstDate)
                        .append("tiktok_first_date", state.ttFirstDate);

                collection.findOneAndReplace(
                        Filters.eq("theme", theme),
                        mongoDoc,
                        new FindOneAndReplaceOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

            } catch (Exception e) {
                System.err.println("Greška pri upisu u MongoDB za temu: " + theme);
                e.printStackTrace();
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
        System.out.println("Trend Delay Between Platforms Processor Started Successfully.");
    }

    private static List<String> extractTikTokHashtagNames(String hashtagsStr) {
        List<String> names = new ArrayList<>();
        if (hashtagsStr == null || hashtagsStr.isEmpty())
            return names;

        Pattern pattern = Pattern.compile("'name':\\s*'([^']+)'|\"name\":\\s*\"([^\"]+)\"");
        Matcher matcher = pattern.matcher(hashtagsStr);
        while (matcher.find()) {
            String name = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
            if (name != null && !name.isEmpty())
                names.add(name);
        }
        return names;
    }

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
}