"""
Daily Currency Exchange ETL Pipeline
Extract (Frankfurter API) -> Transform -> Load (MongoDB)
Data is passed between tasks using named XComs: raw_rates, transformed_rates.
"""
import logging
from datetime import datetime, timezone

import requests
from pymongo import MongoClient, UpdateOne

# Airflow 3 (Astro Runtime 3.x) uses airflow.sdk; fall back to Airflow 2 imports.
try:
    from airflow.sdk import dag, task, get_current_context, Variable
except ImportError:
    from airflow.decorators import dag, task
    from airflow.operators.python import get_current_context
    from airflow.models import Variable

log = logging.getLogger(__name__)

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = "USD,GBP,JPY"
DB_NAME = "currency_db"
COLLECTION_NAME = "exchange_rates"


@task(task_id="extract_currency_rates")
def extract_currency_rates():
    response = requests.get(
        API_URL,
        params={"base": BASE_CURRENCY, "quotes": TARGET_CURRENCIES},
        timeout=30,
    )
    response.raise_for_status()
    raw_data = response.json()

    if not isinstance(raw_data, list) or not raw_data:
        raise ValueError(f"Unexpected API response: {raw_data!r}")

    ti = get_current_context()["ti"]
    ti.xcom_push(key="raw_rates", value=raw_data)  # named XCom #1

    log.info("Extracted exchange rates: %s", raw_data)


@task(task_id="transform_exchange_rate_data")
def transform_exchange_rate_data():
    ti = get_current_context()["ti"]
    raw_data = ti.xcom_pull(task_ids="extract_currency_rates", key="raw_rates")
    if not raw_data:
        raise ValueError("No raw data found in XCom 'raw_rates'")

    retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    transformed_data = [
        {
            "base_currency": item["base"],
            "target_currency": item["quote"],
            "exchange_rate": float(item["rate"]),
            "rate_date": item["date"],
            "source": "Frankfurter API",
            "retrieved_at": retrieved_at,
        }
        for item in raw_data
    ]

    ti.xcom_push(key="transformed_rates", value=transformed_data)  # named XCom #2
    log.info("Transformed exchange-rate data: %s", transformed_data)


@task(task_id="load_data_into_mongodb")
def load_data_into_mongodb():
    ti = get_current_context()["ti"]
    transformed_data = ti.xcom_pull(
        task_ids="transform_exchange_rate_data", key="transformed_rates"
    )
    if not transformed_data:
        raise ValueError("No transformed data found in XCom 'transformed_rates'")

    mongo_uri = Variable.get("MONGO_URI")

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        collection = client[DB_NAME][COLLECTION_NAME]

        # One document per (base, target, date): reruns update instead of duplicating.
        collection.create_index(
            [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
            unique=True,
            name="uniq_currency_pair_per_day",
        )

        operations = [
            UpdateOne(
                {
                    "base_currency": r["base_currency"],
                    "target_currency": r["target_currency"],
                    "rate_date": r["rate_date"],
                },
                {"$set": r},
                upsert=True,
            )
            for r in transformed_data
        ]
        result = collection.bulk_write(operations)

        log.info(
            "Successfully loaded %d records (inserted=%d, updated=%d)",
            len(transformed_data),
            result.upserted_count,
            result.modified_count,
        )
    finally:
        client.close()


@dag(
    dag_id="currency_exchange_etl",
    description="Daily currency exchange rate ETL pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ETL", "Currency", "MongoDB"],
)
def currency_exchange_pipeline():
    extract = extract_currency_rates()
    transform = transform_exchange_rate_data()
    load = load_data_into_mongodb()

    extract >> transform >> load


currency_exchange_pipeline()