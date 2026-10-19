package com.example.kafka_streams_examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Pretvara sirovu JSON poruku iz Kafka topica u VideoDailyMetric.
 * Pravila odgovaraju normalize_platform_data() iz
 * 3_creator_video_daily_metrics.py.
 *
 * Poruka može biti jedan JSON objekat ili JSON niz objekata.
 * Ako se nazivi polja u topicima razlikuju, menjaju se samo aliasi ispod.
 */
final class MessageParser {

    private static final Logger log = LoggerFactory.getLogger(MessageParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ---- Aliasi polja (prvo pronađeno nenull polje pobeđuje) ----
    private static final String[] VIDEO_ID = { "video_id", "id", "videoId" };
    private static final String[] COLLECTED_AT = { "collected_at", "collected_time", "collectedAt" };
    private static final String[] PUBLISHED_AT = { "published_at", "publishedAt" };
    private static final String[] TITLE = { "title" };
    private static final String[] CHANNEL_TITLE = { "channel_title", "channelTitle" };
    private static final String[] VIEWS = { "views", "view_count", "viewCount", "play_count", "playCount" };
    private static final String[] LIKES = { "likes", "like_count", "likeCount", "digg_count", "diggCount" };
    private static final String[] COMMENTS = { "comments", "comment_count", "commentCount" };
    private static final String[] ENGAGEMENT_RATE = { "engagement_rate", "engagementRate" };
    private static final String[] VIRAL_SCORE = { "viral_score", "viralScore" };

    private static final Pattern EPOCH_SECONDS = Pattern.compile("\\d{10}(\\.\\d+)?");
    private static final Pattern EPOCH_MILLIS = Pattern.compile("\\d{13}");

    private static final ConcurrentHashMap<String, AtomicLong> DROPPED = new ConcurrentHashMap<>();

    private MessageParser() {
    }

    static List<VideoDailyMetric> parse(String platform, String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            drop("empty_message");
            return List.of();
        }
        try {
            JsonNode root = MAPPER.readTree(rawJson);
            List<VideoDailyMetric> result = new ArrayList<>();
            if (root.isArray()) {
                for (JsonNode element : root) {
                    VideoDailyMetric metric = normalize(platform, element);
                    if (metric != null) {
                        result.add(metric);
                    }
                }
            } else {
                VideoDailyMetric metric = normalize(platform, root);
                if (metric != null) {
                    result.add(metric);
                }
            }
            return result;
        } catch (JsonProcessingException e) {
            drop("invalid_json");
            return List.of();
        }
    }

    private static VideoDailyMetric normalize(String platform, JsonNode node) {
        if (node == null || !node.isObject()) {
            drop("not_a_json_object");
            return null;
        }

        String videoId = text(node, VIDEO_ID);
        if (videoId == null) {
            drop("missing_video_id");
            return null;
        }

        Instant collectedInstant = instant(first(node, COLLECTED_AT));
        if (collectedInstant == null) {
            drop("missing_or_invalid_collected_at");
            return null;
        }
        String collectedDate = collectedInstant.atZone(ZoneOffset.UTC).toLocalDate().toString();

        // Batch odbacuje redove gde views/likes/comments nisu >= 0 (NULL takođe ispada
        // iz filtera).
        Long views = longValue(first(node, VIEWS));
        Long likes = longValue(first(node, LIKES));
        Long comments = longValue(first(node, COMMENTS));
        if (views == null || likes == null || comments == null || views < 0 || likes < 0 || comments < 0) {
            drop("missing_or_negative_counts");
            return null;
        }

        // engagement_rate: zadržava se validna pripremljena vrednost, inače (likes +
        // comments) / views.
        Double engagement = doubleValue(first(node, ENGAGEMENT_RATE));
        if (engagement == null || engagement.isNaN() || engagement.isInfinite() || engagement < 0) {
            engagement = views > 0 ? (likes + comments) / (double) views : null;
        }

        // viral_score se NE računa ponovo: zadržava se pripremljena vrednost, a ako nje
        // nema ostaje NULL.
        Double viral = doubleValue(first(node, VIRAL_SCORE));
        if (viral != null && (viral.isNaN() || viral.isInfinite())) {
            viral = null;
        }

        Instant published = instant(first(node, PUBLISHED_AT));

        return new VideoDailyMetric(
                platform,
                videoId,
                collectedDate,
                published == null ? null : published.toString(),
                text(node, TITLE),
                text(node, CHANNEL_TITLE),
                views,
                likes,
                comments,
                engagement,
                viral);
    }

    // ---------------------------------------------------------------- helpers

    private static JsonNode first(JsonNode node, String[] names) {
        for (String name : names) {
            JsonNode value = node.get(name);
            if (value != null && !value.isNull() && !value.isMissingNode()) {
                return value;
            }
        }
        return null;
    }

    private static String text(JsonNode node, String[] names) {
        JsonNode value = first(node, names);
        if (value == null) {
            return null;
        }
        String s = value.asText().trim();
        return s.isEmpty() ? null : s;
    }

    private static Long longValue(JsonNode value) {
        if (value == null) {
            return null;
        }
        if (value.isIntegralNumber()) {
            return value.asLong();
        }
        if (value.isNumber()) {
            return (long) value.asDouble(); // isto kao cast double -> bigint u Spark-u (odsecanje)
        }
        try {
            return new BigDecimal(value.asText().trim()).longValue();
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Double doubleValue(JsonNode value) {
        if (value == null) {
            return null;
        }
        if (value.isNumber()) {
            return value.asDouble();
        }
        try {
            return Double.parseDouble(value.asText().trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Instant instant(JsonNode value) {
        if (value == null) {
            return null;
        }
        if (value.isNumber()) {
            return fromEpoch(value.asDouble());
        }
        String s = value.asText().trim();
        if (s.isEmpty()) {
            return null;
        }
        if (EPOCH_SECONDS.matcher(s).matches()) {
            return fromEpoch(Double.parseDouble(s));
        }
        if (EPOCH_MILLIS.matcher(s).matches()) {
            return Instant.ofEpochMilli(Long.parseLong(s));
        }
        try {
            return OffsetDateTime.parse(s).toInstant();
        } catch (DateTimeException ignored) {
            // probaj sledeći format
        }
        try {
            return LocalDateTime.parse(s.replace(' ', 'T')).toInstant(ZoneOffset.UTC);
        } catch (DateTimeException ignored) {
            // probaj sledeći format
        }
        try {
            return LocalDate.parse(s.length() >= 10 ? s.substring(0, 10) : s)
                    .atStartOfDay(ZoneOffset.UTC)
                    .toInstant();
        } catch (DateTimeException ignored) {
            return null;
        }
    }

    private static Instant fromEpoch(double value) {
        return Math.abs(value) >= 1e12 ? Instant.ofEpochMilli((long) value) : Instant.ofEpochSecond((long) value);
    }

    /** Broji odbačene poruke po razlogu; loguje prvih 10 i svaku 1000-tu. */
    private static void drop(String reason) {
        long total = DROPPED.computeIfAbsent(reason, r -> new AtomicLong()).incrementAndGet();
        if (total <= 10 || total % 1000 == 0) {
            log.warn("Dropped input record, reason={}, total for this reason={}", reason, total);
        }
    }
}