from pathlib import Path
import sys

import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ============================================================
# PROJECT
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(
    0,
    str(PROJECT_ROOT)
)

from src.processing.sentiment import analyze_sentiment


# ============================================================
# CONFIG
# ============================================================

def load_config():

    config_path = (
        PROJECT_ROOT
        / "config"
        / "config.yml"
    )

    with open(
        config_path,
        "r",
        encoding="utf-8"
    ) as file:

        return yaml.safe_load(file)


# ============================================================
# SPARK
# ============================================================

def create_spark_session():

    return (
        SparkSession.builder

        .appName(
            "SilverSentimentPipeline"
        )

        .master(
            "local[2]"
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

        .config(
            "spark.python.worker.faulthandler.enabled",
            "true"
        )

        .config(
            "spark.sql.execution.pyspark.udf.faulthandler.enabled",
            "true"
        )

        .getOrCreate()
    )


# ============================================================
# MAIN
# ============================================================

def main():

    config = load_config()

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )


    # ========================================================
    # PATHS
    # ========================================================

    silver_path = str(
        PROJECT_ROOT
        / config["delta"]["silver_path"]
    )

    sentiment_path = str(
        PROJECT_ROOT
        / config["delta"]["sentiment_path"]
    )

    checkpoint_path = str(
        PROJECT_ROOT
        / config["delta"]["sentiment_checkpoint"]
    )


    # ========================================================
    # READ SILVER
    # ========================================================

    silver_df = (
        spark.readStream
        .format("delta")
        .load(silver_path)
        .coalesce(2)
    )


    # ========================================================
    # SENTIMENT ANALYSIS
    # ========================================================

    enriched_df = (
        silver_df

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


    # ========================================================
    # WRITE ENRICHED SILVER
    # ========================================================

    query = (
        enriched_df.writeStream

        .format("delta")

        .outputMode(
            "append"
        )

        .option(
            "checkpointLocation",
            checkpoint_path
        )

        .start(
            sentiment_path
        )
    )


    query.awaitTermination()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        print()
        print(
            "Arrêt Silver Sentiment."
        )