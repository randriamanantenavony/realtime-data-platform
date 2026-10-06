import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


sys.stdout.reconfigure(
    encoding="utf-8"
)


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

sys.path.insert(
    0,
    str(PROJECT_ROOT)
)


from src.processing.sentiment import analyze_sentiment


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

    .appName(
        "TestDistributedSentiment"
    )

    .master(
        "local[*]"
    )

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


spark.sparkContext.setLogLevel(
    "WARN"
)


# ============================================================
# READ SILVER
# ============================================================

silver_df = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)


# ============================================================
# SMALL TEST
# ============================================================

test_df = (
    silver_df
    .filter(
        col("text").isNotNull()
    )
    .limit(50)
)


# ============================================================
# SENTIMENT
# ============================================================

enriched_df = (
    test_df

    .withColumn(
        "sentiment_result",
        analyze_sentiment(
            col("text")
        )
    )

    .withColumn(
        "sentiment",
        col(
            "sentiment_result.sentiment"
        )
    )

    .withColumn(
        "sentiment_score",
        col(
            "sentiment_result.sentiment_score"
        )
    )

    .drop(
        "sentiment_result"
    )
)


# ============================================================
# RESULT
# ============================================================

enriched_df.select(
    "event_id",
    "language",
    "text",
    "sentiment",
    "sentiment_score"
).show(
    20,
    truncate=100
)


spark.stop()