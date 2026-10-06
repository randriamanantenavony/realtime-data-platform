from pathlib import Path

import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    trim
)


# ============================================================
# PROJECT
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]


# ============================================================
# TECH KEYWORDS
# ============================================================

TECH_KEYWORDS = [

    # AI
    "artificial intelligence",
    "chatgpt",
    "openai",
    "copilot",
    "gemini",
    "claude",
    "grok",
    "llm",
    "machine learning",
    "deep learning",
    "generative ai",
    "genai",

    # Data
    "data science",
    "data engineering",
    "big data",
    "spark",
    "pyspark",
    "kafka",
    "databricks",
    "delta lake",

    # Cloud
    "cloud",
    "azure",
    "aws",
    "google cloud",

    # Programming
    "python",
    "java",
    "javascript",
    "typescript",

    # Companies / platforms
    "microsoft",
    "nvidia",
    "github"
]


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
            "BronzeToSilverTechEvents"
        )

        .master(
            "local[*]"
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
            "spark.driver.host",
            "127.0.0.1"
        )
        .config(
            "spark.driver.bindAddress",
            "127.0.0.1"
        )
        .getOrCreate()
    )


# ============================================================
# TECH FILTER
# ============================================================

def build_tech_condition():
    condition = None
    for keyword in TECH_KEYWORDS:
        keyword_condition = (
            lower(col("text"))
            .contains(keyword.lower())
        )
        if condition is None:
            condition = keyword_condition
        else:
            condition = (
                condition
                | keyword_condition
            )

    return condition

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
    bronze_path = str(
        PROJECT_ROOT
        / config["delta"]["bronze_path"]
    )
    silver_path = str(
        PROJECT_ROOT
        / config["delta"]["silver_path"]
    )
    checkpoint_path = str(
        PROJECT_ROOT
        / config["delta"]["silver_checkpoint"]
    )
    # ========================================================
    # READ BRONZE AS STREAM
    # ========================================================
    bronze_df = (
        spark.readStream

        .format("delta")

        .load(bronze_path)
    )
    # ========================================================
    # 1. REMOVE NULL / EMPTY TEXT
    # ========================================================
    clean_df = (
        bronze_df
        .filter(
            col("text").isNotNull()
        )
        .filter(
            trim(col("text")) != ""
        )
    )


    # ========================================================
    # 2. FILTER TECH POSTS
    # ========================================================

    tech_condition = build_tech_condition()

    tech_df = (
        clean_df

        .filter(
            tech_condition
        )
    )
    # ========================================================
    # 3. DROP DUPLICATES
    # ========================================================
    silver_df = (
        tech_df

        .dropDuplicates([
            "event_id"
        ])
    )

    # query_debug = (
    # silver_df.writeStream
    # .format("console")
    # .outputMode("append")
    # .option("truncate", False)
    # .start()
    # )

    # query_debug.awaitTermination() 
    # ========================================================
    # WRITE SILVER
    # ========================================================
    query = (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            checkpoint_path
        )
        .start(
            silver_path
        )
    )
    query.awaitTermination()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        print("Arrêt Bronze -> Silver.")