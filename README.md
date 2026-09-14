# Cross-Platform Gaming Content Analytics (YouTube & TikTok)

This project is developed as part of a master's thesis and focuses on analyzing **gaming content** across **YouTube** and **TikTok**, combining large historical datasets with continuously collected (periodic) data. The system supports both **batch processing** of historical data and **stream processing** of daily-scraped data, and is built on a distributed architecture that includes Apache Kafka, Apache Kafka Streams, Apache Spark, Apache Airflow, Hadoop (HDFS), MongoDB, and Apache Superset for visualization.

The target persona is a gaming content creator who publishes on multiple platforms and wants data-driven insight into virality, cross-platform performance, and optimal content format/timing in order to optimize his content creation strategy.

---

## Architecture

```
Historical data (YouTube, TikTok)
        │
        ▼
    Airflow ──► Spark (batch) ──► HDFS (Raw zone → Transformation zone, Parquet) ──┐
                                                                                     ▼
                                                                               MongoDB (Curated zone) ──► Superset
                                                                                     ▲
Scrapers (YT/TT monitoring, YT/TT search) ──► Kafka (topics) ──► Kafka Streams ─────┘
        ▲
    Airflow (scheduled daily data ingestion)
```

**Components:**

- **Apache Kafka** &ndash; distributed message broker; separate topics for each periodic data source (`yt-monitoring-topic`, `tt-monitoring-topic`, `yt-search-topic`, `tt-search-topic`)
- **Apache Kafka Streams** &ndash; real-time analytical processing of the topics above (topology implemented in Java, module `Kafka-Streams`)
- **Apache Spark** &ndash; batch engine used for the historical analytical queries, run as PySpark jobs
- **Apache Airflow** &ndash; orchestration; schedules the daily scraper → dataset → Kafka pipeline and triggers the historical batch DAGs
- **Hadoop (HDFS)** &ndash; intermediate storage for historical data, structured into a **Raw zone** and a **Transformation zone** (Parquet)
- **MongoDB** &ndash; **Curated zone**; stores the final, aggregated results of both batch and stream processing
- **Apache Superset** &ndash; visualization and dashboarding on top of the curated MongoDB collections
- **Scrapers** &ndash; Python (YouTube Data API v3) and browser-automation (Playwright, Apify) scripts that collect monitoring and search data once a day for both platforms

---

## Cluster Startup

Navigate to the root folder of the project and run:

```bash
./scripts/cluster_up.sh
```

To stop the cluster:

```bash
./scripts/cluster_down.sh
```

---

## Batch Processing (Historical Data)

Historical DAGs can be triggered via the **Airflow GUI** (available at http://localhost:8080) or via CLI commands inside the Airflow container. The jobs read the historical CSV datasets, process them with Spark, stage intermediate results as Parquet on HDFS, and write the final aggregates to MongoDB.

**9 historical analytical queries** are implemented (`Airflow/files/spark/analytical_jobs`):

1. Top gaming videos by year
2. Top gaming channels by year
3. Average views and engagement over time
4. Viral score distribution (popular vs. less popular videos)
5. Impact of video duration on viral score
6. Average video duration per platform
7. Average number of tags in the top 100 videos
8. Most frequently used tags in the top 100 videos
9. Most frequent viral tag combinations

---

## Stream Processing (Monitoring & Search Data)

### 1. Data collection

Four scrapers run once a day, orchestrated by Airflow:

- YouTube monitoring scraper (trending, gaming category)
- TikTok monitoring scraper
- YouTube search scraper (keyword-based)
- TikTok search scraper (keyword-based)

Each scraper writes its daily dataset and sends the new records to its corresponding Kafka topic.

### 2. Real-time analytical processing

Kafka Streams applications (`Kafka-Streams` module) consume the topics and continuously compute the real-time analytical queries, writing results to MongoDB. Implemented queries include:

1. Viral score by gaming topic, per platform
2. Trend evolution of the most popular gaming topics over time
3. Most frequent gaming keywords/tags
4. Time to reach peak popularity
5. Time a video stays among the top-ranked
6. Overlap of popular topics between platforms (heatmap)
7. Delay between platforms in the emergence of the same trend
8. Engagement trend over time (likes, comments)
9. Views on weekdays vs. weekends, per platform
10. Emerging topics in the search dataset that are not yet trending

---

## Visualization

All results (both batch and real-time) are available through **Apache Superset** dashboards, organized into four dashboards:

1. **Virality & Performance** &ndash; top videos/channels, engagement over time, viral score distribution
2. **Content Format Optimization** &ndash; duration vs. viral score, average duration/tags, top tags and tag combinations, top keywords
3. **Trend Dynamics & Lifecycle** &ndash; trend evolution per topic, time to peak, time in top rankings, weekday vs. weekend views
4. **Cross-Platform Trend Analysis** &ndash; emerging topics, viral score by topic per platform, platform overlap heatmap, trend delay between platforms

Superset is accessible at `http://localhost:8088` after the cluster is started.

---

## Datasets

| Dataset                                                                                              | Source                                                  | Size    | Records  |
| ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------- | ------- | -------- |
| [YouTube historical](https://www.kaggle.com/datasets/davorhoma/historical-youtube-gaming-dataset)    | YouTube Trending Dataset (Kaggle) + YouTube Data API v3 | ~575 MB | ~400,000 |
| [TikTok historical](https://www.kaggle.com/datasets/davorhoma/historical-tiktok-gaming-dataset)      | TikTok-10M (Hugging Face)                               | ~370 MB | ~140,000 |
| [YouTube monitoring](https://www.kaggle.com/datasets/davorhoma/monitoring-youtube-real-time-dataset) | YouTube Data API v3 (trending, gaming, 11 regions)      | ~22 MB | ~333,098   |
| [TikTok monitoring](https://www.kaggle.com/datasets/davorhoma/monitoring-tiktok-real-time-dataset)   | Apify `clockworks/tiktok-scraper`                       | ~65 MB  | ~83,416  |
| [YouTube search](https://www.kaggle.com/datasets/davorhoma/youtube-search-gaming-dataset)            | YouTube Data API v3 (~100 keywords)                     | ~23 MB | ~30,429   |
| [TikTok search](https://www.kaggle.com/datasets/davorhoma/tiktok-search-gaming-dataset)              | Playwright automation (~100 keywords)                   | ~70 MB | ~176,582  |

Historical data is collected once and used for long-term pattern analysis; monitoring and search datasets are appended to daily to support the real-time and trend-tracking use cases.

---

## Project Structure

```
├── README.md
├── projekat/
│   ├── Airflow/           # DAGs, Spark jobs, scrapers, Airflow configuration
│   │   ├── config/
│   │   ├── dags/
│   │   ├── files/
│   │   │   ├── data/                  # Collected CSV datasets
│   │   │   └── spark/
│   │   │       ├── analytical_jobs/       # Historical and keyword batch queries
│   │   │       └── transformation_jobs/   # YouTube/TikTok data preparation
│   │   ├── scrapers/                  # YouTube/TikTok monitoring and search scrapers
│   │   └── spark-jars/
│   ├── Hadoop/             # HDFS configuration
│   ├── Kafka/              # Kafka broker and configuration
│   ├── Kafka-Streams/      # Java real-time analytical queries
│   ├── MongoDB/            # MongoDB Docker Compose configuration
│   ├── Spark/              # Spark Docker Compose configuration
│   ├── Superset/           # Superset Docker Compose and configuration
│   ├── scripts/            # Scripts for starting/stopping the cluster
│   └── README.md
├── slike/               # Architecture and system diagrams
│   |── Master-rad-Architecture_diagram.png
|   ├── dashboard-1.jpg
|   ├── dashboard-2.jpg
|   ├── dashboard-3.jpg
│   └── dashboard-4.jpg
└── dokumentacija/          # Project documentation
    └── dokumentacija.pdf
```
