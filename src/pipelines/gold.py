from pathlib import Path

import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    lower,
    when
)


TECHNOLOGIES = [
    "chatgpt",
    "openai",
    "copilot",
    "gemini",
    "claude",
    "grok",
    "python",
    "java",
    "spark",
    "pyspark",
    "kafka",
    "databricks",
    "delta lake",
    "azure",
    "aws",
    "google cloud",
    "machine learning",
    "deep learning",
    "generative ai",
    "genai"
]


# ============================================================
# PROJECT
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]


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
            "GoldSentimentPipeline"
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

    sentiment_path = str(
        PROJECT_ROOT
        / config["delta"]["sentiment_path"]
    )

    gold_summary_path = str(
        PROJECT_ROOT
        / config["delta"]["gold_path"]
        / "sentiment_summary"
    )

    gold_language_path = str(
        PROJECT_ROOT
        / config["delta"]["gold_path"]
        / "sentiment_by_language"
    )

    gold_time_path = str(
        PROJECT_ROOT
        / config["delta"]["gold_path"]
        / "sentiment_over_time"
    )

    gold_technology_path = str(
        PROJECT_ROOT
        / config["delta"]["gold_path"]
        / "technology_trends"
    )


    # ========================================================
    # READ ENRICHED SILVER
    # ========================================================

    sentiment_df = (
        spark.read
        .format("delta")
        .load(sentiment_path)
    )


    # ========================================================
    # CREATE SQL VIEW
    # ========================================================

    sentiment_df.createOrReplaceTempView(
        "sentiment_events"
    )


    # ========================================================
    # GOLD : SENTIMENT SUMMARY
    # ========================================================

    sentiment_summary_df = (
        sentiment_df

        .filter(
            col("sentiment").isNotNull()
        )

        .groupBy(
            "sentiment"
        )

        .agg(
            count("*").alias(
                "post_count"
            )
        )

        .orderBy(
            col("post_count").desc()
        )
    )


    # ========================================================
    # DISPLAY RESULT
    # ========================================================

    print()
    print("=== GOLD SENTIMENT SUMMARY ===")

    sentiment_summary_df.show(
        truncate=False
    )


    # ========================================================
    # WRITE GOLD : SENTIMENT SUMMARY
    # ========================================================

    (
        sentiment_summary_df.write

        .format("delta")

        .mode("overwrite")

        .save(
            gold_summary_path
        )
    )

    print()
    print(
        "Gold sentiment_summary créé avec succès."
    )


    # ========================================================
    # GOLD : SENTIMENT BY LANGUAGE
    # ========================================================

    sentiment_by_language_df = spark.sql("""
        SELECT
            language,
            sentiment,
            COUNT(*) AS post_count
        FROM sentiment_events
        WHERE
            language IS NOT NULL
            AND sentiment IS NOT NULL
        GROUP BY
            language,
            sentiment
        ORDER BY
            language,
            post_count DESC
    """)


    print()
    print("=== GOLD SENTIMENT BY LANGUAGE ===")

    sentiment_by_language_df.show(
        100,
        truncate=False
    )


    (
        sentiment_by_language_df.write

        .format("delta")

        .mode("overwrite")

        .save(
            gold_language_path
        )
    )


    print()
    print(
        "Gold sentiment_by_language créé avec succès."
    )


    # ========================================================
    # GOLD : SENTIMENT OVER TIME
    # ========================================================

    sentiment_over_time_df = spark.sql("""
        SELECT
            DATE(timestamp) AS event_date,
            sentiment,
            COUNT(*) AS post_count
        FROM sentiment_events
        WHERE
            timestamp IS NOT NULL
            AND sentiment IS NOT NULL
        GROUP BY
            DATE(timestamp),
            sentiment
        ORDER BY
            event_date,
            post_count DESC
    """)


    print()
    print("=== GOLD SENTIMENT OVER TIME ===")

    sentiment_over_time_df.show(
        100,
        truncate=False
    )


    (
        sentiment_over_time_df.write

        .format("delta")

        .mode("overwrite")

        .save(
            gold_time_path
        )
    )


    print()
    print(
        "Gold sentiment_over_time créé avec succès."
    )


    # ========================================================
    # GOLD : TECHNOLOGY DETECTION
    # ========================================================

    technology_expression = None

    for technology in TECHNOLOGIES:

        condition = (
            lower(col("text"))
            .contains(technology)
        )

        if technology_expression is None:

            technology_expression = when(
                condition,
                technology
            )

        else:

            technology_expression = (
                technology_expression.when(
                    condition,
                    technology
                )
            )


    technology_df = (
        sentiment_df

        .withColumn(
            "technology",
            technology_expression
        )

        .filter(
            col("technology").isNotNull()
        )
    )


    # IMPORTANT :
    # La vue doit être créée APRÈS l'ajout
    # de la colonne "technology".

    technology_df.createOrReplaceTempView(
        "technology_events"
    )


    # ========================================================
    # GOLD : TECHNOLOGY TRENDS
    # ========================================================

    technology_trends_df = spark.sql("""
        SELECT
            technology,
            sentiment,
            COUNT(*) AS post_count
        FROM technology_events
        WHERE
            technology IS NOT NULL
            AND sentiment IS NOT NULL
        GROUP BY
            technology,
            sentiment
        ORDER BY
            post_count DESC
    """)


    print()
    print("=== GOLD TECHNOLOGY TRENDS ===")

    technology_trends_df.show(
        100,
        truncate=False
    )


    (
        technology_trends_df.write

        .format("delta")

        .mode("overwrite")

        .save(
            gold_technology_path
        )
    )


    print()
    print(
        "Gold technology_trends créé avec succès."
    )


    # ========================================================
    # STOP SPARK
    # ========================================================

    spark.stop()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()