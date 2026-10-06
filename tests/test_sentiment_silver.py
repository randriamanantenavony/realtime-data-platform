from pathlib import Path

from pyspark.sql import SparkSession
from transformers import pipeline

import sys

sys.stdout.reconfigure(encoding="utf-8")


# ============================================================
# PROJECT
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = str(
    PROJECT_ROOT
    / "data"
    / "silver"
    / "tech_events"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("TestSentimentSilver")

    .master("local[*]")

    .config(
        "spark.driver.host",
        "127.0.0.1"
    )

    .config(
        "spark.driver.bindAddress",
        "127.0.0.1"
    )

    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )

    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# READ SILVER
# ============================================================

silver_df = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)


# ============================================================
# GET 10 REAL POSTS
# ============================================================

posts = (
    silver_df
    .select(
        "event_id",
        "text",
        "language"
    )
    .where("text IS NOT NULL")
    .limit(10)
    .collect()
)


# ============================================================
# SENTIMENT MODEL
# ============================================================

MODEL_NAME = (
    "cardiffnlp/"
    "twitter-xlm-roberta-base-sentiment"
)

sentiment_analyzer = pipeline(
    "sentiment-analysis",
    model=MODEL_NAME,
    tokenizer=MODEL_NAME
)


# ============================================================
# TEST
# ============================================================

for post in posts:

    result = sentiment_analyzer(
        post["text"]
    )[0]

    print("\n" + "=" * 80)

    print(
        "EVENT_ID :",
        post["event_id"]
    )

    print(
        "LANGUAGE :",
        post["language"]
    )

    print(
        "TEXT     :",
        post["text"][:300]
    )

    print(
        "SENTIMENT:",
        result["label"]
    )

    print(
        "SCORE    :",
        round(result["score"], 4)
    )


spark.stop()