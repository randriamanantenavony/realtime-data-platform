from transformers import pipeline


MODEL_NAME = (
    "cardiffnlp/"
    "twitter-xlm-roberta-base-sentiment"
)


sentiment_analyzer = pipeline(
    "sentiment-analysis",
    model=MODEL_NAME,
    tokenizer=MODEL_NAME
)


texts = [
    "ChatGPT is absolutely amazing!",
    "I hate this new AI update.",
    "Microsoft released a new AI model.",
    "Python est vraiment excellent !",
    "Cette mise à jour est horrible."
]


for text in texts:

    result = sentiment_analyzer(text)[0]

    print()
    print("TEXT      :", text)
    print("SENTIMENT :", result["label"])
    print("SCORE     :", round(result["score"], 4))