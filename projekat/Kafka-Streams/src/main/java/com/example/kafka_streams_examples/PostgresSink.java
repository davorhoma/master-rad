package com.example.kafka_streams_examples;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/**
 * SQL (DDL + upsert) za obe ciljne tabele. Tipovi kolona odgovaraju batch
 * jobovima.
 */
final class PostgresSinks {

    private PostgresSinks() {
    }

    // ------------------------------------------------ creator_video_daily_metrics

    static List<String> videoDailyMetricsDdl(PostgresConfig cfg) {
        String table = cfg.qualifiedTable();
        return List.of(
                "CREATE SCHEMA IF NOT EXISTS " + cfg.quotedSchema(),
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                        + "platform VARCHAR(32) NOT NULL, "
                        + "video_id VARCHAR(255) NOT NULL, "
                        + "collected_at DATE NOT NULL, "
                        + "published_at TIMESTAMP, "
                        + "title TEXT, "
                        + "channel_title TEXT, "
                        + "views BIGINT, "
                        + "likes BIGINT, "
                        + "comments BIGINT, "
                        + "engagement_rate DOUBLE PRECISION, "
                        + "viral_score DOUBLE PRECISION)",
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                        + PostgresConfig.quote(cfg.table() + "_stream_uq")
                        + " ON " + table + " (platform, video_id, collected_at)");
    }

    static PgUpsertProcessor.RowWriter<VideoDailyMetric> videoDailyMetricsWriter(PostgresConfig cfg) {
        String sql = "INSERT INTO " + cfg.qualifiedTable()
                + " (platform, video_id, collected_at, published_at, title, channel_title,"
                + " views, likes, comments, engagement_rate, viral_score)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT (platform, video_id, collected_at) DO UPDATE SET"
                + " published_at = EXCLUDED.published_at,"
                + " title = EXCLUDED.title,"
                + " channel_title = EXCLUDED.channel_title,"
                + " views = EXCLUDED.views,"
                + " likes = EXCLUDED.likes,"
                + " comments = EXCLUDED.comments,"
                + " engagement_rate = EXCLUDED.engagement_rate,"
                + " viral_score = EXCLUDED.viral_score";

        return (connection, v) -> {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, v.platform());
                ps.setString(2, v.videoId());
                ps.setDate(3, Date.valueOf(LocalDate.parse(v.collectedAt())));
                setTimestamp(ps, 4, v.publishedAt());
                setText(ps, 5, v.title());
                setText(ps, 6, v.channelTitle());
                setLong(ps, 7, v.views());
                setLong(ps, 8, v.likes());
                setLong(ps, 9, v.comments());
                setDouble(ps, 10, v.engagementRate());
                setDouble(ps, 11, v.viralScore());
                ps.executeUpdate();
            }
        };
    }

    // ------------------------------------------------ channel_timeline_2

    static List<String> channelTimelineDdl(PostgresConfig cfg) {
        String table = cfg.qualifiedTable();
        return List.of(
                "CREATE SCHEMA IF NOT EXISTS " + cfg.quotedSchema(),
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                        + "platform TEXT NOT NULL, "
                        + "channel_title TEXT NOT NULL, "
                        + "collected_at DATE NOT NULL, "
                        + "views BIGINT, "
                        + "viral_score DOUBLE PRECISION, "
                        + "engagement_rate DOUBLE PRECISION, "
                        + "video_count BIGINT)",
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                        + PostgresConfig.quote(cfg.table() + "_stream_uq")
                        + " ON " + table + " (platform, channel_title, collected_at)");
    }

    static PgUpsertProcessor.RowWriter<ChannelDailyAggregate> channelTimelineWriter(PostgresConfig cfg) {
        String upsert = "INSERT INTO " + cfg.qualifiedTable()
                + " (platform, channel_title, collected_at, views, viral_score, engagement_rate, video_count)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT (platform, channel_title, collected_at) DO UPDATE SET"
                + " views = EXCLUDED.views,"
                + " viral_score = EXCLUDED.viral_score,"
                + " engagement_rate = EXCLUDED.engagement_rate,"
                + " video_count = EXCLUDED.video_count";
        String delete = "DELETE FROM " + cfg.qualifiedTable()
                + " WHERE platform = ? AND channel_title = ? AND collected_at = ?";

        return (connection, agg) -> {
            if (agg.videoCount() <= 0) {
                // Svi videi kanala za taj dan su nestali iz agregata: red se uklanja.
                try (PreparedStatement ps = connection.prepareStatement(delete)) {
                    ps.setString(1, agg.platform());
                    ps.setString(2, agg.channelTitle());
                    ps.setDate(3, Date.valueOf(LocalDate.parse(agg.collectedAt())));
                    ps.executeUpdate();
                }
                return;
            }
            try (PreparedStatement ps = connection.prepareStatement(upsert)) {
                ps.setString(1, agg.platform());
                ps.setString(2, agg.channelTitle());
                ps.setDate(3, Date.valueOf(LocalDate.parse(agg.collectedAt())));
                ps.setLong(4, agg.sumViews());
                ps.setDouble(5, agg.avgViralScore());
                ps.setDouble(6, agg.avgEngagementRate());
                ps.setLong(7, agg.videoCount());
                ps.executeUpdate();
            }
        };
    }

    // ------------------------------------------------ null-safe setteri

    private static void setText(PreparedStatement ps, int index, String value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.VARCHAR);
        } else {
            ps.setString(index, value);
        }
    }

    private static void setLong(PreparedStatement ps, int index, Long value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.BIGINT);
        } else {
            ps.setLong(index, value);
        }
    }

    private static void setDouble(PreparedStatement ps, int index, Double value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.DOUBLE);
        } else {
            ps.setDouble(index, value);
        }
    }

    private static void setTimestamp(PreparedStatement ps, int index, String isoInstant) throws SQLException {
        if (isoInstant == null) {
            ps.setNull(index, Types.TIMESTAMP);
        } else {
            ps.setTimestamp(index, Timestamp.from(Instant.parse(isoInstant)));
        }
    }
}