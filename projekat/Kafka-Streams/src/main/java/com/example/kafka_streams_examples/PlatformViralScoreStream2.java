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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
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

    // --- Konfiguracija za growth-rate (levi grafik, ПП9) ---
    private static final String GROWTH_HISTORY_STORE = "theme-daily-history-store";
    // Jedina konstanta koju treba menjati da bi se promenio broj dana koji se
    // poredi: "poslednjih GROWTH_WINDOW_DAYS dana" vs "GROWTH_WINDOW_DAYS dana
    // pre toga", po LITERALNIM kalendarskim datumima (ne po broju zapisa u
    // istoriji - vidi napomenu u GrowthRateProcessor-u).
    private static final int GROWTH_WINDOW_DAYS = Integer.parseInt(
            System.getenv().getOrDefault("GROWTH_WINDOW_DAYS", "3"));

    // --- Konfiguracija za "already trending" proveru (GRANA 3, ПП9 filter) ---
    private static final String TRENDING_STORE = "canonical-trending-global-store";
    private static final int TRENDING_TOP_N = Integer.parseInt(
            System.getenv().getOrDefault("TRENDING_TOP_N", "15"));

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://admin:admin@localhost:27017");
        String mongoDbName = System.getenv().getOrDefault("MONGO_DB", "realtime_data");
        String mongoCollectionName = System.getenv().getOrDefault("MONGO_COLLECTION", "query_platform_viral_metrics");
        String mongoGrowthCollectionName = System.getenv().getOrDefault("MONGO_GROWTH_COLLECTION",
                "query_emerging_topics");

        // Monitoring (trending) topici i interni topik za GlobalKTable lookup
        String ytMonitoringTopic = System.getenv().getOrDefault("YT_MONITORING_TOPIC", "yt-monitoring-topic");
        String ttMonitoringTopic = System.getenv().getOrDefault("TT_MONITORING_TOPIC", "tt-monitoring-topic");
        String trendingInternalTopic = System.getenv().getOrDefault("TRENDING_INTERNAL_TOPIC",
                "canonical-trending-agg");

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
        // GRANA 2 (postojeća): agregacija po temi+datumu, BEZ platforme
        // -> ulaz za izračunavanje 3-day growth rate-a (levi grafik, ПП9)
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

        // ==========================================================
        // GRANA 3 (nova): agregacija MONITORING (trending) podataka po
        // platformi+temi, direktno iz yt-monitoring-topic i
        // tt-monitoring-topic. Ne koristi se ni za jedan grafik direktno -
        // služi samo kao GlobalKTable lookup pomoću kog GrowthRateProcessor
        // proverava da li je tema VEĆ probila top-N po avg viral_score-u na
        // YouTube-u i/ili TikTok-u, i na osnovu toga isključuje temu iz liste
        // "emerging topics" (ПП9), da se ne bi duplirala sa onim što ПП1 već
        // prikazuje kao trending.
        //
        // GlobalKTable (a ne obična KTable) je neophodan jer monitoring
        // podaci NISU ko-particionisani sa search podacima (drugi topici,
        // drugačiji ključevi) - GlobalKTable se replicira na svaku instancu
        // i dostupan je iz bilo kog Processor-a bez obzira na particionisanje.
        // ==========================================================
        KStream<String, String> ytMonitoring = builder.stream(ytMonitoringTopic);
        KStream<String, String> ttMonitoring = builder.stream(ttMonitoringTopic);

        KStream<String, MonitoringAgg> ytMonitoringMapped = ytMonitoring
                .map((key, value) -> parseMonitoringMessage(value, "youtube"));
        KStream<String, MonitoringAgg> ttMonitoringMapped = ttMonitoring
                .map((key, value) -> parseMonitoringMessage(value, "tiktok"));

        KTable<String, MonitoringAgg> monitoringAggTable = ytMonitoringMapped.merge(ttMonitoringMapped)
                .filter((k, v) -> v != null && !k.startsWith("ERROR"))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(MonitoringAgg.class)))
                .reduce((aggValue, newValue) -> new MonitoringAgg(
                        newValue.platform,
                        newValue.theme,
                        aggValue.viralScoreSum + newValue.viralScoreSum,
                        aggValue.count + newValue.count));

        // Objavljivanje changelog-a na interni (compacted) topik - potrebno
        // da bi se od njega mogao napraviti GlobalKTable. Topik treba da
        // postoji unapred (ili da je auto.create.topics.enable=true na
        // broker-u); u produkciji ga je bolje eksplicitno kreirati sa
        // cleanup.policy=compact.
        monitoringAggTable.toStream().to(trendingInternalTopic,
                Produced.with(Serdes.String(), new JsonSerde<>(MonitoringAgg.class)));

        GlobalKTable<String, MonitoringAgg> trendingGlobalTable = builder.globalTable(
                trendingInternalTopic,
                Materialized.<String, MonitoringAgg, KeyValueStore<Bytes, byte[]>>as(TRENDING_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new JsonSerde<>(MonitoringAgg.class)));

        StoreBuilder<KeyValueStore<String, String>> historyStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(GROWTH_HISTORY_STORE),
                Serdes.String(),
                Serdes.String());
        builder.addStateStore(historyStoreBuilder);

        // KRITIČNO: themeAggregatedTable je repartition-ovan po ključu
        // "tema_datum" (vidi .groupBy iznad, GRANA 2) - to znači da dva
        // različita datuma ZA ISTU TEMU mogu završiti na dve različite Kafka
        // particije, obrađene od strane dva različita Streams task-a, od
        // kojih svaki ima SVOJU lokalnu (particionisanu) kopiju
        // GROWTH_HISTORY_STORE-a. Rezultat: GrowthRateProcessor bi video samo
        // ONAJ podskup dana za temu koji je slučajno hashovan na njegovu
        // particiju - istorija bi bila fragmentisana i prosek pogrešan (npr.
        // prior_3day_avg bi ispao jednak vrednosti samo JEDNOG dana), umesto
        // proseka nad svim danima koji stvarno postoje u prozoru.
        //
        // Zato se stream OVDE eksplicitno re-key-uje na "samo tema" i
        // eksplicitno repartition-uje (.repartition(...)) - ovo pravi novi
        // interni repartition topik ključan po temi, garantujući da SVI
        // datumi za istu temu završe na ISTOJ particiji/task-u, i time dele
        // ISTU lokalnu instancu GROWTH_HISTORY_STORE-a.
        KStream<String, PlatformMetrics> themeKeyedStream = themeAggregatedTable.toStream()
                .selectKey((key, value) -> value.theme)
                .repartition(Repartitioned.<String, PlatformMetrics>as("theme-daily-history-repartition")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new JsonSerde<>(PlatformMetrics.class)));

        // Napomena: TRENDING_STORE (GlobalKTable) se NE dodaje ovde kao
        // connected store - globalni store-ovi su automatski dostupni iz bilo
        // kog Processor-a preko context.getStateStore(), bez potrebe da se
        // eksplicitno navedu u .process(...) pozivu.
        KStream<String, GrowthResult> growthStream = themeKeyedStream
                .process(GrowthRateProcessor::new, GROWTH_HISTORY_STORE);

        growthStream.foreach((theme, result) -> {
            try {
                Document doc = new Document("topic", result.theme)
                        .append("date", result.date)
                        .append("growth_rate", result.growthRate)
                        .append("recent_3day_avg_viral_score", result.recentAvg)
                        .append("prior_3day_avg_viral_score", result.priorAvg);

                // Upsert SAMO po temi (bez datuma u ključu) - ovde nam ne treba
                // istorija growth rate-ova kroz vreme, već samo TRENUTNA vrednost
                // za tu temu. "date" ostaje u dokumentu kao informativno polje
                // (na osnovu kog dana je poslednji put izračunato), ali se ne
                // koristi za pronalaženje dokumenta - zato svaki novi izračun
                // PREPISUJE prethodni, umesto da pravi novi dokument.
                growthCollection.updateOne(
                        com.mongodb.client.model.Filters.eq("topic", result.theme),
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

    /**
     * Parsira monitoring (trending) poruku u agregat po platformi+temi.
     * Koristi istu ekstrakciju polja i istu normalizaciju teme kao
     * {@link #parseMessage(String, String)}, ali bez datuma - agregat je
     * kumulativan (isto kao u ViralScoreByGamingThemeStream-u) jer služi
     * samo za rangiranje tema po platformi, ne za trend kroz vreme.
     */
    private static KeyValue<String, MonitoringAgg> parseMonitoringMessage(String value, String platform) {
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
            double viralScore = node.has("viral_score") ? node.get("viral_score").asDouble(0.0) : 0.0;

            String groupKey = canonicalTheme + "_" + platform;

            return KeyValue.pair(groupKey, new MonitoringAgg(platform, canonicalTheme, viralScore, 1));
        } catch (Exception e) {
            log.error("Greška pri parsiranju monitoring poruke sa platforme {}: {}", platform, value, e);
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

    /**
     * Kumulativni agregat po platformi+temi, izveden iz monitoring
     * (trending) topika. Koristi se isključivo kao "lookup" podatak
     * (GlobalKTable) da bi se utvrdilo koje su teme već trending, radi
     * filtriranja emerging-topics liste (ПП9). Nema datum jer nam ovde
     * treba samo rang po ukupnom proseku, ne trend kroz vreme.
     */
    public static class MonitoringAgg {
        public String platform;
        public String theme;
        public double viralScoreSum;
        public long count;

        public MonitoringAgg() {
        }

        public MonitoringAgg(String platform, String theme, double viralScoreSum, long count) {
            this.platform = platform;
            this.theme = theme;
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
     * Za svaku temu čuva rolling istoriju dnevnih proseka u state store-u (kao
     * JSON string), ograničenu na poslednjih 2*GROWTH_WINDOW_DAYS kalendarskih
     * dana. Growth rate poredi prosek za poslednjih GROWTH_WINDOW_DAYS
     * kalendarskih dana (računajući od STVARNOG današnjeg datuma, ne od
     * datuma poruke) sa prosekom za prethodnih GROWTH_WINDOW_DAYS kalendarskih
     * dana pre toga - i prosleđuje rezultat dalje ako je tema još uvek
     * "emerging" (vidi {@link #isAlreadyTrending(String)}).
     *
     * VAŽNO: prozori su vezani za STVARNE kalendarske datume, sa anchor-om =
     * {@code LocalDate.now()} (npr. za GROWTH_WINDOW_DAYS=3 i danas=13.9:
     * recent=[11.9-13.9], prior=[8.9-10.9]), a ne za "poslednjih N zapisa u
     * istoriji" niti za datum poruke koja je poslednja stigla za tu temu -
     * ovo je namerno, jer poruke mogu stizati sa zakašnjenjem ili sa
     * "collected_at"/"trending_detected_time" iz prošlosti, pa bi anchor
     * vezan za datum poruke davao pogrešan prozor u odnosu na stvarni "danas".
     * Ako tema nema podatke baš svaki dan (uobičajeno kod scraping
     * pipeline-a), prosek se računa samo nad danima koji stvarno postoje u
     * datom rasponu, umesto da se prozor tiho pomeri dalje unazad da bi
     * upotpunio fiksan broj zapisa. Ako u nekom od dva prozora nema nijednog
     * dana sa podacima, growth rate se ne računa za tu temu (bez forward-a).
     *
     * Pošto je anchor sada vezan za stvarni "danas" a ne za dolazeće poruke,
     * sam dolazak poruke više nije dovoljan da drži growth rate ažurnim - ako
     * tema nema NIJEDNU novu poruku tokom dana, prozor bi i dalje trebalo da
     * "kliza" napred svaki dan. Zato se, pored recompute-a pri svakoj
     * pristigloj poruci, registruje i dnevni WALL_CLOCK_TIME punktuator koji
     * prolazi kroz SVE teme u istoriji i ponovo računa growth rate u odnosu
     * na tekući datum, bez obzira na to da li je za tu temu baš tog dana
     * stigla nova poruka.
     */
    public static class GrowthRateProcessor implements Processor<String, PlatformMetrics, String, GrowthResult> {
        private KeyValueStore<String, String> store;
        // NAPOMENA: store-ovi koji stoje iza KTable/GlobalKTable-a (kao
        // TRENDING_STORE) interno čuvaju vrednosti omotane u
        // ValueAndTimestamp<V>, bez obzira šta se navede u Materialized
        // generičkim parametrima. Zato se OVDE mora koristiti
        // ValueAndTimestamp<MonitoringAgg>, a ne "goli" MonitoringAgg -
        // u suprotnom se dobija ClassCastException pri svakom čitanju.
        private KeyValueStore<String, ValueAndTimestamp<MonitoringAgg>> trendingStore;
        private ProcessorContext<String, GrowthResult> context;

        @Override
        public void init(ProcessorContext<String, GrowthResult> context) {
            this.context = context;
            this.store = context.getStateStore(GROWTH_HISTORY_STORE);
            // TRENDING_STORE je backing store GlobalKTable-a - dostupan je iz
            // bilo kog Processor-a u topologiji bez potrebe da se eksplicitno
            // navede kao "connected store" u .process(...) pozivu.
            this.trendingStore = context.getStateStore(TRENDING_STORE);

            // Dnevni recompute za SVE teme, nezavisno od toga da li je za njih
            // baš danas stigla nova poruka - inače bi prozor ostao "zaleđen"
            // na poslednjem danu kad je tema imala saobraćaj.
            //
            // VAŽNO: context.schedule(interval, ...) prvi put "tikne" TEK
            // POSLE proteklog intervala od starta procesora, ne odmah. To
            // znači da bi, bez nekog prvog poziva uskoro po startu, dokumenti
            // u Mongo-u za teme bez novog saobraćaja posle restarta ostali
            // "zaleđeni" na vrednosti izračunatoj PRE restarta sve dok prvi
            // punktuator ne tikne - i do 24h kasnije.
            //
            // NE SME se, međutim, pozvati recomputeAllThemes() direktno OVDE
            // u init() - u tom trenutku downstream čvorovi topologije još
            // nisu "otvoreni" (topologija se tek inicijalizuje), pa
            // context.forward() (pozvan iznutra) baca
            // "IllegalStateException: The processor is already closed".
            // Zato se početni recompute radi kroz JEDNOKRATNI punktuator sa
            // kratkim odlaganjem (izvršava se tek kad je topologija potpuno
            // spremna za forward), koji sam sebe otkazuje posle prvog
            // izvršavanja, nakon čega ostaje samo redovni 24h punktuator.
            final Cancellable[] startupPunctuator = new Cancellable[1];
            startupPunctuator[0] = context.schedule(java.time.Duration.ofSeconds(5),
                    PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                        recomputeAllThemes();
                        startupPunctuator[0].cancel();
                    });
            context.schedule(java.time.Duration.ofHours(24), PunctuationType.WALL_CLOCK_TIME,
                    timestamp -> recomputeAllThemes());
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

                history = trimHistory(history);
                store.put(value.theme, objectMapper.writeValueAsString(history));

                computeAndForward(value.theme, history, record.timestamp());
            } catch (Exception e) {
                log.error("Greška pri računanju growth rate-a za temu: {}", value.theme, e);
            }
        }

        /**
         * Prolazi kroz istoriju SVIH tema u store-u i ponovo računa growth
         * rate za svaku od njih u odnosu na trenutni datum (LocalDate.now()).
         * Poziva se jednom dnevno iz WALL_CLOCK_TIME punktuatora, da bi
         * prozori "klizili" napred i za teme koje tog dana nemaju nijednu
         * novu poruku.
         */
        private void recomputeAllThemes() {
            long now = System.currentTimeMillis();
            try (KeyValueIterator<String, String> it = store.all()) {
                while (it.hasNext()) {
                    KeyValue<String, String> kv = it.next();
                    String theme = kv.key;
                    try {
                        List<DailyPoint> history = kv.value == null
                                ? new ArrayList<>()
                                : objectMapper.readValue(kv.value, new TypeReference<List<DailyPoint>>() {
                                });
                        history = trimHistory(history);
                        store.put(theme, objectMapper.writeValueAsString(history));
                        computeAndForward(theme, history, now);
                    } catch (Exception e) {
                        log.error("Greška pri dnevnom recompute-u growth rate-a za temu: {}", theme, e);
                    }
                }
            }
        }

        /**
         * Uklanja iz istorije dane starije od 2*GROWTH_WINDOW_DAYS kalendarskih
         * dana unazad od STVARNOG današnjeg datuma (a ne od fiksnog BROJA
         * zapisa), da JSON lista u store-u ne raste neograničeno kod
         * dugotrajno aktivnih tema.
         */
        private List<DailyPoint> trimHistory(List<DailyPoint> history) {
            LocalDate anchor = LocalDate.now();
            LocalDate cutoff = anchor.minusDays(2L * GROWTH_WINDOW_DAYS);
            history.removeIf(p -> LocalDate.parse(p.date, DateTimeFormatter.ISO_LOCAL_DATE).isBefore(cutoff));
            return history;
        }

        /**
         * Računa growth rate za datu temu na osnovu njene istorije, poredeći
         * prozor [danas - (GROWTH_WINDOW_DAYS-1), danas] sa prozorom
         * [danas - (2*GROWTH_WINDOW_DAYS-1), danas - GROWTH_WINDOW_DAYS], gde
         * je "danas" = LocalDate.now() (stvarni kalendarski datum, ne datum
         * poslednje poruke). Dani bez podataka se jednostavno ne nalaze u
         * istoriji i zato ne ulaze u prosek - prosek se računa samo nad
         * danima koji STVARNO postoje u datom rasponu.
         */
        private void computeAndForward(String theme, List<DailyPoint> history, long timestamp) {
            LocalDate anchor = LocalDate.now();

            LocalDate recentStart = anchor.minusDays(GROWTH_WINDOW_DAYS - 1L);
            LocalDate priorStart = anchor.minusDays(2L * GROWTH_WINDOW_DAYS - 1L);
            LocalDate priorEnd = anchor.minusDays(GROWTH_WINDOW_DAYS);

            List<DailyPoint> recentPoints = new ArrayList<>();
            List<DailyPoint> priorPoints = new ArrayList<>();
            for (DailyPoint p : history) {
                LocalDate d = LocalDate.parse(p.date, DateTimeFormatter.ISO_LOCAL_DATE);
                if (!d.isBefore(recentStart) && !d.isAfter(anchor)) {
                    recentPoints.add(p);
                } else if (!d.isBefore(priorStart) && !d.isAfter(priorEnd)) {
                    priorPoints.add(p);
                }
            }

            // Zahtevamo BAR PO JEDAN dan podataka u oba prozora - inače growth
            // rate nema smisla (npr. tema se pojavila tek juče, nema šta da
            // se poredi sa "prethodnih GROWTH_WINDOW_DAYS dana").
            if (recentPoints.isEmpty() || priorPoints.isEmpty()) {
                return;
            }

            double recentAvg = average(recentPoints);
            double priorAvg = average(priorPoints);
            double growthRate = priorAvg > 0 ? (recentAvg - priorAvg) / priorAvg : 0.0;

            if (isAlreadyTrending(theme)) {
                log.info(
                        "Tema '{}' preskočena za ПП9 - već je u top-{} na obe platforme (AND logika).",
                        theme, TRENDING_TOP_N);
                return;
            }

            GrowthResult result = new GrowthResult(theme, anchor.toString(), growthRate, recentAvg, priorAvg);
            context.forward(new Record<>(theme, result, timestamp));
        }

        /**
         * AND logika: tema se smatra "već trending" (i isključuje se iz
         * emerging liste, ПП9) SAMO ako je istovremeno probila top-N po
         * avg viral_score-u i na YouTube-u I na TikTok-u. Ako je probila
         * top-N na samo jednoj platformi (ili ni na jednoj), i dalje se
         * tretira kao "emerging" kandidat.
         *
         * Za labaviju (OR) varijantu u budućnosti: zameniti telo petlje sa
         * "if (isInTopN(theme, platform)) return true;" i na kraju
         * "return false;" - tj. isključiti čim je top-N probijen na BILO
         * KOJOJ platformi.
         */
        private boolean isAlreadyTrending(String theme) {
            if (trendingStore == null) {
                return false; // GlobalKTable store još nije popunjen (npr. odmah po startu app-a)
            }

            for (String platform : List.of("youtube", "tiktok")) {
                if (!isInTopN(theme, platform)) {
                    return false; // promašio top-N na BAR JEDNOJ platformi -> ostaje emerging (AND)
                }
            }
            return true; // probio top-N na OBE platforme -> više nije emerging
        }

        /**
         * Rangira sve teme za datu platformu po avg viral_score-u (iz
         * TRENDING_STORE-a) i proverava da li je data tema u top-N. Katalog
         * kanonskih tema je mali (desetak-do-stotinak fiksnih kategorija iz
         * mapToCanonicalTheme), pa je puno skeniranje store-a pri svakom
         * pozivu jeftino i ne zahteva keširanje.
         */
        private boolean isInTopN(String theme, String platform) {
            List<Map.Entry<String, Double>> ranked = new ArrayList<>();
            try (KeyValueIterator<String, ValueAndTimestamp<MonitoringAgg>> it = trendingStore.all()) {
                while (it.hasNext()) {
                    KeyValue<String, ValueAndTimestamp<MonitoringAgg>> kv = it.next();
                    MonitoringAgg agg = kv.value == null ? null : kv.value.value();
                    if (agg == null || agg.count <= 0 || !platform.equals(agg.platform)) {
                        continue;
                    }
                    ranked.add(Map.entry(agg.theme, agg.viralScoreSum / agg.count));
                }
            }
            ranked.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));
            return ranked.stream()
                    .limit(TRENDING_TOP_N)
                    .anyMatch(e -> e.getKey().equals(theme));
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