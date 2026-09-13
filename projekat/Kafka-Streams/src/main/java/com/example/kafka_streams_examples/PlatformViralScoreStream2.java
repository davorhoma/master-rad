package com.example.kafka_streams_examples;

import com.fasterxml.jackson.core.type.TypeReference;
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
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class PlatformViralScoreStream2 {
    private static final Logger log = LoggerFactory.getLogger(PlatformViralScoreStream2.class);
    private static final ObjectMapper objectMapper = com.fasterxml.jackson.databind.json.JsonMapper.builder()
            .enable(com.fasterxml.jackson.core.json.JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
            .build();

    // --- Konfiguracija za growth-rate (levi grafik) ---
    private static final String GROWTH_HISTORY_STORE = "theme-daily-history-store";
    private static final int WINDOW_DAYS = 3; // "3-day growth rate"
    private static final int REQUIRED_HISTORY_DAYS = WINDOW_DAYS * 2; // 3 pre + 3 posle

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://admin:admin@localhost:27017");
        String mongoDbName = System.getenv().getOrDefault("MONGO_DB", "realtime_data");
        String mongoCollectionName = System.getenv().getOrDefault("MONGO_COLLECTION", "query_platform_viral_metrics");
        String mongoGrowthCollectionName = System.getenv().getOrDefault("MONGO_GROWTH_COLLECTION",
                "query_emerging_topics");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-processor-platform-viral-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(mongoDbName);
        MongoCollection<Document> collection = database.getCollection(mongoCollectionName);
        MongoCollection<Document> growthCollection = database.getCollection(mongoGrowthCollectionName);

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
                .filter((k, v) -> v != null && !k.startsWith("ERROR"));

        // ==========================================================
        // GRANA 1 (postojeća): agregacija po temi+platformi+datumu
        // -> desni grafik (viral score trend po platformi)
        // ==========================================================
        KTable<String, PlatformMetrics> aggregatedTable = allMapped
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(PlatformMetrics.class)))
                .reduce((aggValue, newValue) -> {
                    double newViralSum = aggValue.viralScoreSum + newValue.viralScoreSum;
                    long newCount = aggValue.count + newValue.count;
                    return new PlatformMetrics(
                            newValue.theme,
                            newValue.platform,
                            newValue.date,
                            newViralSum,
                            newCount);
                });

        aggregatedTable.toStream().foreach((key, value) -> {
            try {
                double avgViralScore = value.count > 0 ? value.viralScoreSum / value.count : 0.0;

                Document doc = new Document("topic", value.theme)
                        .append("platform", value.platform)
                        .append("date", value.date)
                        .append("avg_viral_score", avgViralScore)
                        .append("total_records", value.count);

                collection.updateOne(
                        com.mongodb.client.model.Filters.and(
                                com.mongodb.client.model.Filters.eq("topic", value.theme),
                                com.mongodb.client.model.Filters.eq("platform", value.platform),
                                com.mongodb.client.model.Filters.eq("date", value.date)),
                        new Document("$set", doc),
                        new UpdateOptions().upsert(true));

                log.info("Platforma: {}, Tema: {}, Datum: {}, Prosečan viral score: {}",
                        value.platform, value.theme, value.date, avgViralScore);
            } catch (Exception e) {
                log.error("Greška pri upisu u MongoDB za temu: {}", value.theme, e);
            }
        });

        // ==========================================================
        // GRANA 2 (nova): agregacija po temi+datumu, BEZ platforme
        // -> ulaz za izračunavanje 3-day growth rate-a (levi grafik)
        // ==========================================================
        KTable<String, PlatformMetrics> themeAggregatedTable = allMapped
                .groupBy((key, value) -> value.theme + "_" + value.date,
                        Grouped.with(Serdes.String(), new JsonSerde<>(PlatformMetrics.class)))
                .reduce((aggValue, newValue) -> {
                    double newViralSum = aggValue.viralScoreSum + newValue.viralScoreSum;
                    long newCount = aggValue.count + newValue.count;
                    return new PlatformMetrics(
                            newValue.theme,
                            "all",
                            newValue.date,
                            newViralSum,
                            newCount);
                });

        StoreBuilder<KeyValueStore<String, String>> historyStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(GROWTH_HISTORY_STORE),
                Serdes.String(),
                Serdes.String());
        builder.addStateStore(historyStoreBuilder);

        KStream<String, GrowthResult> growthStream = themeAggregatedTable.toStream()
                .process(GrowthRateProcessor::new, GROWTH_HISTORY_STORE);

        growthStream.foreach((theme, result) -> {
            try {
                Document doc = new Document("topic", result.theme)
                        .append("date", result.date)
                        .append("growth_rate", result.growthRate)
                        .append("recent_3day_avg_viral_score", result.recentAvg)
                        .append("prior_3day_avg_viral_score", result.priorAvg);

                growthCollection.updateOne(
                        com.mongodb.client.model.Filters.and(
                                com.mongodb.client.model.Filters.eq("topic", result.theme),
                                com.mongodb.client.model.Filters.eq("date", result.date)),
                        new Document("$set", doc),
                        new UpdateOptions().upsert(true));

                log.info("Emerging topic kandidat — Tema: {}, Datum: {}, Growth rate: {}",
                        result.theme, result.date, result.growthRate);
            } catch (Exception e) {
                log.error("Greška pri upisu growth rate-a za temu: {}", result.theme, e);
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

    /**
     * Proverava da li dato polje u JSON čvoru sadrži upotrebljiv tekst.
     * Tretira kao "nema vrednosti" sledeće slučajeve:
     * - polje ne postoji
     * - polje je JSON null
     * - polje je numerička NaN vrednost (neki producer-i, npr. Python json.dumps,
     * šalju NaN za nedostajuće hashtag-ove umesto null-a ili praznog stringa)
     * - polje je prazan string ili doslovno string "NaN"
     */
    private static boolean isUsableText(JsonNode node, String field) {
        if (node == null || !node.has(field) || node.get(field).isNull()) {
            return false;
        }
        JsonNode fieldNode = node.get(field);
        if (fieldNode.isNumber() && Double.isNaN(fieldNode.doubleValue())) {
            return false;
        }
        String text = fieldNode.asText();
        return text != null && !text.isBlank() && !"NaN".equalsIgnoreCase(text.trim());
    }

    private static KeyValue<String, PlatformMetrics> parseMessage(String value, String platform) {
        try {
            JsonNode node = objectMapper.readTree(value);

            String rawTag = "";
            if (isUsableText(node, "tags")) {
                rawTag = node.get("tags").asText();
            } else if (isUsableText(node, "hashtags")) {
                rawTag = node.get("hashtags").asText();
            } else if (isUsableText(node, "description")) {
                rawTag = node.get("description").asText();
            } else if (isUsableText(node, "title")) {
                rawTag = node.get("title").asText();
            }

            String canonicalTheme = mapToCanonicalTheme(rawTag);

            // Ekstrakcija datuma zavisno od platforme
            String timeField = null;
            if ("youtube".equals(platform)) {
                if (node.has("collected_at")) {
                    timeField = node.get("collected_at").asText();
                }
            } else if ("tiktok".equals(platform)) {
                if (node.has("trending_detected_time")) {
                    timeField = node.get("trending_detected_time").asText();
                }
            }

            // TODO: Možda izbaciti LocalDate.now(), pa ukoliko timeField ne postoji, da se
            // ta poruka prebaci u ERROR.
            String dateStr = LocalDate.now().toString();
            if (timeField != null) {
                try {
                    OffsetDateTime odt = OffsetDateTime.parse(timeField);
                    dateStr = odt.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE);
                } catch (Exception ignored) {
                }
            }

            double viralScore = node.has("viral_score") ? node.get("viral_score").asDouble(0.0) : 0.0;

            // Ključ sada sadrži i datum kako bi se podaci particionisali i čuvali po danima
            String streamKey = canonicalTheme + "_" + platform + "_" + dateStr;

            return KeyValue.pair(streamKey,
                    new PlatformMetrics(canonicalTheme, platform, dateStr, viralScore, 1));
        } catch (Exception e) {
            log.error("Greška pri parsiranju poruke sa platforme {}: {}", platform, value, e);
            return KeyValue.pair("ERROR_" + System.currentTimeMillis(), null);
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
        public String date;
        public double viralScoreSum;
        public long count;

        public PlatformMetrics() {
        }

        public PlatformMetrics(String theme, String platform, String date, double viralScoreSum, long count) {
            this.theme = theme;
            this.platform = platform;
            this.date = date;
            this.viralScoreSum = viralScoreSum;
            this.count = count;
        }
    }

    // ==========================================================
    // Klase i procesor za growth-rate izračunavanje (levi grafik)
    // ==========================================================

    /** Jedna dnevna tačka proseka viral score-a za temu. */
    public static class DailyPoint {
        public String date;
        public double avgScore;

        public DailyPoint() {
        }

        public DailyPoint(String date, double avgScore) {
            this.date = date;
            this.avgScore = avgScore;
        }
    }

    /** Rezultat koji se upisuje u Mongo za levi grafik. */
    public static class GrowthResult {
        public String theme;
        public String date;
        public double growthRate;
        public double recentAvg;
        public double priorAvg;

        public GrowthResult() {
        }

        public GrowthResult(String theme, String date, double growthRate, double recentAvg, double priorAvg) {
            this.theme = theme;
            this.date = date;
            this.growthRate = growthRate;
            this.recentAvg = recentAvg;
            this.priorAvg = priorAvg;
        }
    }

    /**
     * Za svaku temu čuva rolling istoriju od poslednjih REQUIRED_HISTORY_DAYS
     * dnevnih proseka u state store-u (kao JSON string). Kada ima dovoljno
     * istorije, računa growth rate poredeći prosek poslednja WINDOW_DAYS dana
     * sa prosekom prethodna WINDOW_DAYS dana i prosleđuje rezultat dalje.
     *
     * Napomena: pošto se dnevni prosek (aggregatedTable po temi+datumu) ažurira
     * postepeno kako pristižu poruke tog dana, ovaj procesor će se pozivati
     * više puta u toku dana za isti datum — svaki put samo update-uje "današnji"
     * unos u istoriji i prepravlja growth rate. To je očekivano i baš to daje
     * "skoro real-time" ponašanje u Supersetu.
     */
    public static class GrowthRateProcessor implements Processor<String, PlatformMetrics, String, GrowthResult> {
        private KeyValueStore<String, String> store;
        private ProcessorContext<String, GrowthResult> context;

        @Override
        public void init(ProcessorContext<String, GrowthResult> context) {
            this.context = context;
            this.store = context.getStateStore(GROWTH_HISTORY_STORE);
        }

        @Override
        public void process(Record<String, PlatformMetrics> record) {
            PlatformMetrics value = record.value();
            if (value == null || value.count <= 0) {
                return;
            }

            try {
                double avgScore = value.viralScoreSum / value.count;

                String historyJson = store.get(value.theme);
                List<DailyPoint> history = historyJson == null
                        ? new ArrayList<>()
                        : objectMapper.readValue(historyJson, new TypeReference<List<DailyPoint>>() {
                        });

                // Upsert današnjeg unosa (ako već postoji za taj datum, prepiši ga)
                history.removeIf(p -> p.date.equals(value.date));
                history.add(new DailyPoint(value.date, avgScore));
                history.sort(Comparator.comparing(p -> p.date));

                // Drži samo poslednjih REQUIRED_HISTORY_DAYS dana po temi
                while (history.size() > REQUIRED_HISTORY_DAYS) {
                    history.remove(0);
                }

                store.put(value.theme, objectMapper.writeValueAsString(history));

                if (history.size() == REQUIRED_HISTORY_DAYS) {
                    double priorAvg = average(history.subList(0, WINDOW_DAYS));
                    double recentAvg = average(history.subList(WINDOW_DAYS, REQUIRED_HISTORY_DAYS));
                    double growthRate = priorAvg > 0 ? (recentAvg - priorAvg) / priorAvg : 0.0;

                    GrowthResult result = new GrowthResult(value.theme, value.date, growthRate, recentAvg, priorAvg);
                    context.forward(new Record<>(value.theme, result, record.timestamp()));
                }
            } catch (Exception e) {
                log.error("Greška pri računanju growth rate-a za temu: {}", value.theme, e);
            }
        }

        @Override
        public void close() {
        }

        private static double average(List<DailyPoint> points) {
            return points.stream().mapToDouble(p -> p.avgScore).average().orElse(0.0);
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