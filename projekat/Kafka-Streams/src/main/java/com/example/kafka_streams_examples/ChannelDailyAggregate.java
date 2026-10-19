package com.example.kafka_streams_examples;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Dnevni agregat jednog kanala (platform, channel_title, collected_at).
 * Nepromenljiv (immutable): add/subtract vraćaju novu instancu.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ChannelDailyAggregate(
        @JsonProperty("platform") String platform,
        @JsonProperty("channel_title") String channelTitle,
        @JsonProperty("collected_at") String collectedAt,
        @JsonProperty("video_count") long videoCount,
        @JsonProperty("sum_views") long sumViews,
        @JsonProperty("sum_viral_score") double sumViralScore,
        @JsonProperty("sum_engagement_rate") double sumEngagementRate) {

    public static ChannelDailyAggregate empty() {
        return new ChannelDailyAggregate(null, null, null, 0L, 0L, 0d, 0d);
    }

    /** Adder: dodaje video u agregat. */
    public ChannelDailyAggregate add(VideoDailyMetric v) {
        return new ChannelDailyAggregate(
                v.platform(),
                v.channelTitle(),
                v.collectedAt(),
                videoCount + 1,
                sumViews + v.views(),
                sumViralScore + v.viralScore(),
                sumEngagementRate + v.engagementRate());
    }

    /** Subtractor: uklanja prethodnu verziju videa iz agregata. */
    public ChannelDailyAggregate subtract(VideoDailyMetric v) {
        long remaining = videoCount - 1;
        if (remaining <= 0) {
            // Poslednji video je uklonjen: sume se resetuju na tačnu nulu (bez akumulirane
            // greške).
            return new ChannelDailyAggregate(v.platform(), v.channelTitle(), v.collectedAt(), 0L, 0L, 0d, 0d);
        }
        return new ChannelDailyAggregate(
                v.platform(),
                v.channelTitle(),
                v.collectedAt(),
                remaining,
                sumViews - v.views(),
                sumViralScore - v.viralScore(),
                sumEngagementRate - v.engagementRate());
    }

    /**
     * viral_score: prosek zaokružen na 4 decimale (kao F.round(F.avg(..), 4) u
     * batch-u).
     */
    public double avgViralScore() {
        return videoCount == 0 ? 0d : round(sumViralScore / videoCount, 4);
    }

    /** engagement_rate: prosek zaokružen na 6 decimala. */
    public double avgEngagementRate() {
        return videoCount == 0 ? 0d : round(sumEngagementRate / videoCount, 6);
    }

    private static double round(double value, int scale) {
        return BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_UP).doubleValue();
    }
}