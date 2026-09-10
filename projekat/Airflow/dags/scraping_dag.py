from datetime import datetime, timedelta
import json
import os
import sys
import time
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
from kafka import KafkaProducer

DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAGS_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from scrapers.tt_playwright_scraper1 import main as tt_playwright_scraper1
from scrapers.tt_playwright_scraper2 import main as tt_playwright_scraper2
from scrapers.tt_scraper import main as tt_monitoring_scraper
from scrapers.yt_scraper import main as yt_monitoring_scraper
from scrapers.yt_query_scraper1 import main as yt_query_scraper1
from scrapers.yt_query_scraper2 import main as yt_query_scraper2


def _get_last_sent_index(state_file_path):
    if os.path.exists(state_file_path):
        with open(state_file_path, "r") as f:
            data = json.load(f)
            return data.get("last_index", 0)
    return 0


def _save_last_sent_index(state_file_path, index):
    with open(state_file_path, "w") as f:
        json.dump({"last_index": index}, f)


def _send_file_to_kafka_incremental(file_name, topic_env_key, default_topic):
    bootstrap_servers = os.getenv(
        "BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092"
    ).split(",")
    topic_name = os.getenv(topic_env_key, default_topic)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        batch_size=16384,  # Veličina batch-a u bajtovima (default je 16KB)
        linger_ms=50,  # Čeka do 50ms da nakupi poruke pre slanja (poboljšava protok)
        compression_type="gzip",
    )

    SCRAPERS_DIR = os.getenv("SCRAPERS_DIR", "/opt/airflow/scrapers")
    full_file_path = os.path.join(SCRAPERS_DIR, file_name)
    state_file_path = os.path.join(SCRAPERS_DIR, f"{file_name}.offset.json")

    try:
        print(f"Čitanje fajla {full_file_path} i slanje na topik: {topic_name}...")
        df = pd.read_csv(full_file_path)
        records = df.to_dict(orient="records")
        total_records = len(records)

        last_sent_index = _get_last_sent_index(state_file_path)
        if last_sent_index >= total_records:
            print("Nema novih redova za slanje.")
            return

        new_records = records[last_sent_index:]
        print(
            f"Pronađeno {len(new_records)} novih redova (od indeksa {last_sent_index}"
            f" do {total_records})."
        )

        for i, row_dict in enumerate(new_records):
            # producer.send je asinhron - ne blokira izvršavanje, već stavlja poruku u memorijski bafer
            producer.send(topic_name, value=row_dict)

            # (Opcionalno) Ako želiš da povremeno ispuniš bafer ili da ispišeš progres
            if (i + 1) % 500 == 0:
                print(f"Poslato {i + 1} / {len(new_records)} poruka...")

        producer.flush()
        _save_last_sent_index(state_file_path, total_records)
        print(
            f"Uspešno završeno slanje svih {total_records} poruka za fajl {file_name} na topik {topic_name} i sačuvan novi offset: {total_records}"
        )

    except FileNotFoundError:
        print(f"Upozorenje: Fajl {full_file_path} nije pronađen.")
    except Exception as e:
        print(f"Greška pri slanju fajla {full_file_path}: {e}")
        raise e
    finally:
        producer.close()


def scrape_youtube_monitoring():
    # Vaš kod za scrapovanje YouTube monitoring-a i čuvanje u CSV
    print("Pokrecem yt_monitoring_scraper")
    yt_monitoring_scraper()


def scrape_tiktok_monitoring():
    print("Pokrecem tt_monitoring_scraper")
    tt_monitoring_scraper()


def scrape_youtube_search():
    # Vaš kod za scrapovanje TikTok monitoring-a i čuvanje u CSV
    print("Pokrecem yt_search_scraper1")
    yt_query_scraper1()

    time.sleep(1)
    print("Pokrecem yt_search_scraper2")
    yt_query_scraper2()


def scrape_tiktok_search():
    # Vaš kod za scrapovanje TikTok monitoring-a i čuvanje u CSV
    print("Pokrecem tt_playwright_scraper1")
    tt_playwright_scraper1()

    time.sleep(1)
    print("Pokrecem tt_playwright_scraper2")
    tt_playwright_scraper2()


TIKTOK_MONITORING_OUTPUT_FILE = os.getenv(
    "TIKTOK_APIFY_OUTPUT_FILE", "apify_spojeno.csv"
)
TIKTOK_SEARCH_OUTPUT_FILE = os.getenv(
    "TIKTOK_PLAYWRIGHT_OUTPUT_FILE", "playwright_spojeno.csv"
)
YOUTUBE_MONITORING_OUTPUT_FILE = os.getenv(
    "YOUTUBE_MONITORING_OUTPUT_FILE", "youtube_gaming_trending_spojeno.csv"
)
YOUTUBE_SEARCH_OUTPUT_FILE = os.getenv(
    "YOUTUBE_QUERY_OUTPUT_FILE", "youtube_search_gaming_spojeno.csv"
)


def send_yt_monitoring_to_kafka():
    _send_file_to_kafka_incremental(
        YOUTUBE_MONITORING_OUTPUT_FILE, "YT_MONITORING_TOPIC", "yt-monitoring-topic"
    )


def send_tt_monitoring_to_kafka():
    _send_file_to_kafka_incremental(
        TIKTOK_MONITORING_OUTPUT_FILE, "TT_MONITORING_TOPIC", "tt-monitoring-topic"
    )


def send_yt_search_to_kafka():
    _send_file_to_kafka_incremental(
        YOUTUBE_SEARCH_OUTPUT_FILE, "YT_SEARCH_TOPIC", "yt-search-topic"
    )


def send_tt_search_to_kafka():
    _send_file_to_kafka_incremental(
        TIKTOK_SEARCH_OUTPUT_FILE, "TT_SEARCH_TOPIC", "tt-search-topic"
    )


default_args = {
    "owner": "master_rad",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

local_tz = pendulum.timezone("Europe/Belgrade")

with DAG(
    "youtube_tiktok_pipeline",
    default_args=default_args,
    description="Scraping u 17h i slanje na Kafku",
    schedule="0 17 * * *",  # Pokreće se svaki dan u tačno 17:00h
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    catchup=False,
) as dag:

    t_yt_mon = PythonOperator(
        task_id="scrape_youtube_monitoring",
        python_callable=scrape_youtube_monitoring,
    )

    t_tt_mon = PythonOperator(
        task_id="scrape_tiktok_monitoring",
        python_callable=scrape_tiktok_monitoring,
    )

    t_yt_search = PythonOperator(
        task_id="scrape_youtube_search",
        python_callable=scrape_youtube_search,
    )

    t_tt_search = PythonOperator(
        task_id="scrape_tiktok_search",
        python_callable=scrape_tiktok_search,
    )

    t_send_yt_mon = PythonOperator(
        task_id="send_yt_monitoring_to_kafka",
        python_callable=send_yt_monitoring_to_kafka,
    )

    t_send_tt_mon = PythonOperator(
        task_id="send_tt_monitoring_to_kafka",
        python_callable=send_tt_monitoring_to_kafka,
    )

    t_send_yt_search = PythonOperator(
        task_id="send_yt_search_to_kafka",
        python_callable=send_yt_search_to_kafka,
    )

    t_send_tt_search = PythonOperator(
        task_id="send_tt_search_to_kafka",
        python_callable=send_tt_search_to_kafka,
    )

    # Sva 4 skrejpera rade u paraleli i svaki od njih pokreće svoj Kafka producer task
    t_yt_mon >> t_send_yt_mon
    t_tt_mon >> t_send_tt_mon
    t_yt_search >> t_send_yt_search
    t_tt_search >> t_send_tt_search
    # scrapers = [t_yt_mon, t_tt_mon, t_yt_search, t_tt_search]
    # kafka_senders = [t_send_yt_mon, t_send_tt_mon, t_send_yt_search, t_send_tt_search]
