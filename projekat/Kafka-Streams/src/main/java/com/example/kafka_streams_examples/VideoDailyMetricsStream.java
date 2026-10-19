package com.example.kafka_streams_examples;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

/**
 * APP 1: yt-monitoring-topic, yt-search-topic, tt-monitoring-topic,
 * tt-search-topic
 * -> normalizacija -> deduplikacija po (platform, video_id, collected_at)
 * -> PostgreSQL creator_video_daily_metrics (upsert) + izlazni topic za App 2.
 */
public class VideoDailyMetricsStream {

    public static void main(String[] args) {
        List<String> youtubeTopics = AppConfig.envList("INPUT_YT_TOPICS", "yt-monitoring-topic,yt-search-topic");
        List<String> tiktokTopics = AppConfig.envList("INPUT_TT_TOPICS", "tt-monitoring-topic,tt-search-topic");
        String outputTopic = AppConfig.env("OUTPUT_TOPIC", "creator-video-daily-metrics");
        PostgresConfig postgres = PostgresConfig.fromEnv("creator_video_daily_metrics");

        Topology topology = buildTopology(youtubeTopics, tiktokTopics, outputTopic, postgres);
        StreamsSupport.run(topology, StreamsSupport.baseProperties("creator-video-daily-metrics-app"));
    }

    static Topology buildTopology(
            List<String> youtubeTopics, List<String> tiktokTopics, String outputTopic, PostgresConfig postgres) {

        StreamsBuilder builder = new StreamsBuilder();
        Serde<VideoDailyMetric> metricSerde = new JsonSerde<>(VideoDailyMetric.class);

        // 1. Čitanje i normalizacija. Platforma se određuje po topicu (yt-* / tt-*).
        KStream<String, VideoDailyMetric> youtube = builder
                .stream(youtubeTopics, Consumed.with(Serdes.String(), Serdes.String()).withName("source-youtube"))
                .flatMapValues(raw -> MessageParser.parse("YouTube", raw), Named.as("normalize-youtube"));

        KStream<String, VideoDailyMetric> tiktok = builder
                .stream(tiktokTopics, Consumed.with(Serdes.String(), Serdes.String()).withName("source-tiktok"))
                .flatMapValues(raw -> MessageParser.parse("TikTok", raw), Named.as("normalize-tiktok"));

        // 2. Deduplikacija: jedan red po (platform, video_id, collected_at).
        // Zadržava se red sa više popunjenih polja, a zatim sa više
        // views/likes/comments.
        KTable<String, VideoDailyMetric> daily = youtube
                .merge(tiktok, Named.as("merge-platforms"))
                .selectKey((key, metric) -> metric.key(), Named.as("key-by-video-day"))
                .groupByKey(Grouped.with("video-day", Serdes.String(), metricSerde))
                .reduce(
                        VideoDailyMetricsStream::pickBetter,
                        Named.as("dedup-video-day"),
                        Materialized.<String, VideoDailyMetric, KeyValueStore<Bytes, byte[]>>as("video-day-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(metricSerde));

        KStream<String, VideoDailyMetric> updates = daily.toStream(Named.as("video-day-updates"));

        // 3a. Izlazni topic (ulaz za App 2); isti ključ => compacted topic čuva
        // poslednje stanje.
        updates.to(outputTopic, Produced.with(Serdes.String(), metricSerde));

        // 3b. Upsert u PostgreSQL.
        List<String> ddl = PostgresSinks.videoDailyMetricsDdl(postgres);
        PgUpsertProcessor.RowWriter<VideoDailyMetric> writer = PostgresSinks.videoDailyMetricsWriter(postgres);
        updates.process(() -> new PgUpsertProcessor<VideoDailyMetric>(postgres, ddl, writer),
                Named.as("postgres-upsert"));

        return builder.build();
    }

    /**
     * Reducer: vraća bolji od dva zapisa za isti (platform, video_id,
     * collected_at). Pri izjednačenju ostaje postojeći.
     */
    static VideoDailyMetric pickBetter(VideoDailyMetric current, VideoDailyMetric candidate) {
        return compare(candidate, current) > 0 ? candidate : current;
    }

    private static int compare(VideoDailyMetric a, VideoDailyMetric b) {
        int result = Integer.compare(a.completeness(), b.completeness());
        if (result != 0)
            return result;
        result = Long.compare(nullLast(a.views()), nullLast(b.views()));
        if (result != 0)
            return result;
        result = Long.compare(nullLast(a.likes()), nullLast(b.likes()));
        if (result != 0)
            return result;
        return Long.compare(nullLast(a.comments()), nullLast(b.comments()));
    }

    private static long nullLast(Long value) {
        return value == null ? Long.MIN_VALUE : value;
    }
}