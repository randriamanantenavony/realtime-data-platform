import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ============================================================
# CONFIGURATION
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
# SPARK SESSION
# ============================================================
def create_spark_session():

    return (
        SparkSession.builder
        .appName("RealtimeSocialDataPlatform")
        .master("local[*]")
        .getOrCreate()
    )

# ============================================================
# SCHEMA 
# ============================================================

def get_event_schema():

    return StructType([
        StructField("event_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("author_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("text", StringType(), True),
        StructField("language", StringType(), True),
        StructField("url", StringType(), True),
        StructField("ingested_at", StringType(), True)
    ])


# ============================================================
# KAFKA STREAM
# ============================================================
def read_kafka_stream(
    spark,
    bootstrap_servers,
    topic
):
    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            bootstrap_servers
        )
        .option(
            "subscribe",
            topic
        )
        .option(
            "startingOffsets",
            "latest"
        )
        .load()
    )


# ============================================================
# MAIN
# ============================================================

def main():

    config = load_config()

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")


    # --------------------------------------------------------
    # Kafka
    # --------------------------------------------------------

    kafka_df = read_kafka_stream(

        spark,

        config["kafka"]["bootstrap_servers"],

        config["kafka"]["topic"]
    )


    # --------------------------------------------------------
    # Kafka retourne value sous forme binaire.
    # On la convertit en String.
    # --------------------------------------------------------

    events_df = kafka_df.selectExpr(
        "CAST(value AS STRING) AS json_value",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp"
    )

    event_schema = get_event_schema()
    parsed_df = (
        events_df
        .withColumn(
            "data",
            from_json(
                col("json_value"),
                event_schema
            )
        )
    )   
    final_df = parsed_df.select(
    "data.*",
    "topic",
    "partition",
    "offset",
    "kafka_timestamp"
)


    # --------------------------------------------------------
    # Affichage console uniquement pour validation
    # --------------------------------------------------------

    query = (
        final_df.writeStream
        .format("console")
        .outputMode("append")
        .option(
            "truncate",
            "false"
        ).start()
    )


    query.awaitTermination()


if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        print()
        print("Arrêt du streaming Spark.")