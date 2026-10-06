from typing import Iterator

import pandas as pd

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    DoubleType
)


MODEL_NAME = (
    "cardiffnlp/"
    "twitter-xlm-roberta-base-sentiment"
)


sentiment_schema = StructType([
    StructField(
        "sentiment",
        StringType(),
        True
    ),
    StructField(
        "sentiment_score",
        DoubleType(),
        True
    )
])


@pandas_udf(sentiment_schema)
def analyze_sentiment(
    batches: Iterator[pd.Series]
) -> Iterator[pd.DataFrame]:

    # Import inside worker
    from transformers import pipeline

    # Loaded once for this worker execution
    sentiment_analyzer = pipeline(
        "sentiment-analysis",
        model=MODEL_NAME,
        tokenizer=MODEL_NAME
    )

    for texts in batches:

        clean_texts = (
            texts
            .fillna("")
            .astype(str)
            .tolist()
        )

        results = sentiment_analyzer(
            clean_texts,
            truncation=True,
            batch_size=16
        )

        yield pd.DataFrame({
            "sentiment": [
                result["label"].lower()
                for result in results
            ],

            "sentiment_score": [
                float(result["score"])
                for result in results
            ]
        })