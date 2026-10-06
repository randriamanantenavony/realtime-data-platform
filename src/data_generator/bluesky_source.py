import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime, timezone

import yaml
import websockets


# ============================================================
# Permet d'importer les modules depuis src/
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SRC_DIR = PROJECT_ROOT / "src"

sys.path.insert(
    0,
    str(SRC_DIR)
)


from producers.kafka_producer import KafkaEventProducer


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
# 1. BLUESKY CLIENT
# ============================================================

class BlueskyClient:

    def __init__(self, jetstream_url):
        self.jetstream_url = jetstream_url

    async def connect(self):

        print("Connexion à Bluesky Jetstream...")

        websocket = await websockets.connect(
            self.jetstream_url
        )

        print("Bluesky connecté.")

        return websocket


# ============================================================
# 2. BLUESKY EVENT TRANSFORMER
# ============================================================

class BlueskyEventTransformer:

    def transform(self, data):

        # Seulement les commits
        if data.get("kind") != "commit":
            return None

        commit = data.get("commit", {})

        # Seulement les nouveaux événements
        if commit.get("operation") != "create":
            return None

        record = commit.get("record", {})

        # Seulement les publications
        if record.get("$type") != "app.bsky.feed.post":
            return None

        text = record.get("text", "")

        if not text:
            return None

        return {

            "event_id": commit.get("rkey"),

            "source": "bluesky",

            "author_id": data.get("did"),

            "timestamp": record.get("createdAt"),

            "text": text,

            "language": self._extract_language(record),

            "url": None,

            "ingested_at": datetime.now(
                timezone.utc
            ).isoformat()
        }

    def _extract_language(self, record):

        languages = record.get(
            "langs",
            []
        )

        if languages:
            return languages[0]

        return None


# ============================================================
# 3. BLUESKY STREAM LISTENER
# ============================================================

class BlueskyStreamListener:

    def __init__(
        self,
        client,
        transformer,
        producer
    ):

        self.client = client
        self.transformer = transformer
        self.producer = producer

    async def start(self):

        websocket = await self.client.connect()

        print("Ecoute des événements Bluesky...")
        print()

        try:

            while True:

                # --------------------------------------------
                # Récupération WebSocket
                # --------------------------------------------

                message = await websocket.recv()

                data = json.loads(message)


                # --------------------------------------------
                # Normalisation
                # --------------------------------------------

                event = self.transformer.transform(
                    data
                )

                if event is None:
                    continue


                # --------------------------------------------
                # Kafka
                # --------------------------------------------

                self.producer.send(
                    event
                )


        finally:

            await websocket.close()

            self.producer.close()


# ============================================================
# MAIN
# ============================================================

async def main():

    # --------------------------------------------------------
    # Charger configuration
    # --------------------------------------------------------

    config = load_config()


    # --------------------------------------------------------
    # Bluesky
    # --------------------------------------------------------

    bluesky_client = BlueskyClient(
        config["bluesky"]["jetstream_url"]
    )


    # --------------------------------------------------------
    # Transformer
    # --------------------------------------------------------

    transformer = BlueskyEventTransformer()


    # --------------------------------------------------------
    # Kafka Producer
    # --------------------------------------------------------

    kafka_producer = KafkaEventProducer(

        bootstrap_servers=
            config["kafka"]["bootstrap_servers"],

        topic=
            config["kafka"]["topic"]
    )


    # --------------------------------------------------------
    # Listener
    # --------------------------------------------------------

    listener = BlueskyStreamListener(
        bluesky_client,
        transformer,
        kafka_producer
    )


    # --------------------------------------------------------
    # GO
    # --------------------------------------------------------

    await listener.start()


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print()
        print("Arrêt du pipeline.")