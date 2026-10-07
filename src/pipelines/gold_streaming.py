from pathlib import Path

import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    when
)


# ============================================================
# PROJECT
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]


# ============================================================
# TECHNOLOGIES
# ============================================================

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
            "GoldStreamingPipeline"
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

    spark.sparkContext.setLogLevel("WARN")


    # ========================================================
    # PATHS
    # ========================================================

    sentiment_path = str(
        PROJECT_ROOT
        / config["delta"]["sentiment_path"]
    )

    gold_root = (
        PROJECT_ROOT
        / config["delta"]["gold_path"]
    )

    gold_summary_path = str(
        gold_root / "sentiment_summary"
    )

    gold_language_path = str(
        gold_root / "sentiment_by_language"
    )

    gold_time_path = str(
        gold_root / "sentiment_over_time"
    )

    gold_technology_path = str(
        gold_root / "technology_trends"
    )

    checkpoint_path = str(
        PROJECT_ROOT
        / config["delta"]["gold_checkpoint"]
    )


    # ========================================================
    # READ SILVER SENTIMENT AS STREAM
    # ========================================================

    sentiment_stream = (
        spark.readStream
        .format("delta")
        .load(sentiment_path)
    )


    # ========================================================
    # PROCESS EACH MICRO-BATCH
    # ========================================================

    def update_gold(batch_df, batch_id):

        # Ignore empty micro-batches
        if batch_df.isEmpty():
            return

        print()
        print(
            f"=== GOLD MICRO-BATCH {batch_id} ==="
        )

        # ----------------------------------------------------
        # IMPORTANT
        # ----------------------------------------------------
        # Gold is a snapshot of ALL Silver sentiment data,
        # not only the current micro-batch.
        # ----------------------------------------------------

        full_df = (
            spark.read
            .format("delta")
            .load(sentiment_path)
        )

        full_df.createOrReplaceTempView(
            "sentiment_events"
        )


        # ====================================================
        # 1. SENTIMENT SUMMARY
        # ====================================================

        print("1/4 - Calcul sentiment_summary...")
        summary_df = spark.sql("""
            SELECT
                sentiment,
                COUNT(*) AS post_count
            FROM sentiment_events
            WHERE sentiment IS NOT NULL
            GROUP BY sentiment
            ORDER BY post_count DESC
        """)

        print("2/4 - Ecriture sentiment_summary...")
        (
            summary_df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_summary_path)
        )
        print("2/4 - Ecriture sentiment_summary terminée.")

        # ====================================================
        # 2. SENTIMENT BY LANGUAGE
        # ====================================================

        print("3/4 - Calcul sentiment_by_language...")
        language_df = spark.sql("""
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

        print("4/4 - Ecriture sentiment_by_language...")
        (
            language_df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_language_path)
        )
        print("4/4 - Ecriture sentiment_by_language terminée.")

        # ====================================================
        # 3. SENTIMENT OVER TIME
        # ====================================================
        
        print("5/4 - Calcul sentiment_over_time...")
        time_df = spark.sql("""
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
        print("6/4 - Ecriture sentiment_over_time...")

        (
            time_df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_time_path)
        )
        print("6/4 - Ecriture sentiment_over_time terminée.")

        # ====================================================
        # 4. TECHNOLOGY TRENDS
        # ====================================================

        print("7/4 - Calcul technology_trends...")  
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

        
        technology_events_df = (
            full_df

            .withColumn(
                "technology",
                technology_expression
            )

            .filter(
                col("technology").isNotNull()
            )
        )


        technology_events_df.createOrReplaceTempView(
            "technology_events"
        )

        print("8/4 - Ecriture technology_trends...")
        technology_df = spark.sql("""
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

        print("8/4 - Ecriture technology_trends...")
        (
            technology_df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_technology_path)
        )


        print(
            f"Gold actualisé - micro-batch {batch_id}"
        )


    # ========================================================
    # START STREAM
    # ========================================================

    query = (
        sentiment_stream.writeStream

        .foreachBatch(
            update_gold
        )

        .option(
            "checkpointLocation",
            checkpoint_path
        )

        .trigger(
            processingTime="10 seconds"
        ).start()
    )


    print()
    print("Gold Streaming démarré.")
    print("Actualisation toutes les 10 secondes.")
    query.awaitTermination()


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        print("Arrêt Gold Streaming.")