package com.example.kafka_streams_examples;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

/**
 * APP 2: izlaz App 1 (jedan red po video/dan) -> agregacija po (platform,
 * channel_title, collected_at)
 * -> PostgreSQL channel_timeline_2 (upsert).
 *
 * Ulaz se čita kao KTable, pa izmena videa ne duplira pregledi: groupBy +
 * aggregate
 * poziva subtractor za staru vrednost i adder za novu.
 */
public class ChannelTimelineStream {

    public static void main(String[] args) {
        String inputTopic = AppConfig.env("INPUT_TOPIC", "creator-video-daily-metrics");
        PostgresConfig postgres = PostgresConfig.fromEnv("channel_timeline_2");

        Topology topology = buildTopology(inputTopic, postgres);
        StreamsSupport.run(topology, StreamsSupport.baseProperties("channel-timeline-app"));
    }

    static Topology buildTopology(String inputTopic, PostgresConfig postgres) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<VideoDailyMetric> metricSerde = new JsonSerde<>(VideoDailyMetric.class);
        Serde<ChannelDailyAggregate> aggregateSerde = new JsonSerde<>(ChannelDailyAggregate.class);

        KTable<String, VideoDailyMetric> videos = builder.table(
                inputTopic,
                Consumed.with(Serdes.String(), metricSerde).withName("source-video-day"),
                Materialized.<String, VideoDailyMetric, KeyValueStore<Bytes, byte[]>>as("video-day-source-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(metricSerde));

        KTable<String, ChannelDailyAggregate> channelDaily = videos
                // Pravila čišćenja iz 1_channel_timeline.py (red mora imati kanal, views,
                // viral_score, engagement_rate).
                .filter((key, metric) -> isEligible(metric), Named.as("timeline-eligible"))
                .groupBy(
                        (key, metric) -> KeyValue.pair(channelDayKey(metric), metric),
                        Grouped.with("channel-day", Serdes.String(), metricSerde))
                .aggregate(
                        ChannelDailyAggregate::empty,
                        (key, metric, aggregate) -> aggregate.add(metric),
                        (key, metric, aggregate) -> aggregate.subtract(metric),
                        Named.as("aggregate-channel-day"),
                        Materialized
                                .<String, ChannelDailyAggregate, KeyValueStore<Bytes, byte[]>>as("channel-day-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(aggregateSerde));

        List<String> ddl = PostgresSinks.channelTimelineDdl(postgres);
        PgUpsertProcessor.RowWriter<ChannelDailyAggregate> writer = PostgresSinks.channelTimelineWriter(postgres);
        channelDaily
                .toStream(Named.as("channel-day-updates"))
                .process(
                        () -> new PgUpsertProcessor<ChannelDailyAggregate>(postgres, ddl, writer),
                        Named.as("postgres-upsert"));

        return builder.build();
    }

    /**
     * Odgovara filteru iz 1_channel_timeline.py: nema NULL u ključnim poljima i
     * nema negativnih vrednosti.
     */
    static boolean isEligible(VideoDailyMetric metric) {
        return metric != null
                && metric.platform() != null
                && metric.videoId() != null
                && metric.collectedAt() != null
                && metric.channelTitle() != null
                && metric.views() != null
                && metric.viralScore() != null
                && metric.engagementRate() != null
                && metric.views() >= 0
                && metric.viralScore() >= 0
                && metric.engagementRate() >= 0;
    }

    /**
     * Separator \u001f ne može da se nađe u nazivu kanala, pa ključ ostaje
     * jednoznačan.
     */
    static String channelDayKey(VideoDailyMetric metric) {
        return metric.platform() + "\u001f" + metric.channelTitle() + "\u001f" + metric.collectedAt();
    }
}