package com.example.kafka_streams_examples;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Jedan video na jedan dan: odgovara redu u tabeli creator_video_daily_metrics.
 * Datumi su ISO stringovi (collected_at = yyyy-MM-dd, published_at = ISO
 * instant u UTC),
 * pa nije potreban Jackson JavaTime modul.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record VideoDailyMetric(
        @JsonProperty("platform") String platform,
        @JsonProperty("video_id") String videoId,
        @JsonProperty("collected_at") String collectedAt,
        @JsonProperty("published_at") String publishedAt,
        @JsonProperty("title") String title,
        @JsonProperty("channel_title") String channelTitle,
        @JsonProperty("views") Long views,
        @JsonProperty("likes") Long likes,
        @JsonProperty("comments") Long comments,
        @JsonProperty("engagement_rate") Double engagementRate,
        @JsonProperty("viral_score") Double viralScore) {

    /** Prirodni ključ: platform|video_id|collected_at. */
    public String key() {
        return platform + "|" + videoId + "|" + collectedAt;
    }

    /**
     * Isto kao completeness_score u deduplicate_daily_observations (broj popunjenih
     * polja, 0-8).
     */
    public int completeness() {
        int score = 0;
        if (publishedAt != null)
            score++;
        if (title != null)
            score++;
        if (channelTitle != null)
            score++;
        if (views != null)
            score++;
        if (likes != null)
            score++;
        if (comments != null)
            score++;
        if (engagementRate != null)
            score++;
        if (viralScore != null)
            score++;
        return score;
    }
}