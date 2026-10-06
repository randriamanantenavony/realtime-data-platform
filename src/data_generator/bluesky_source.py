import asyncio
import json
import yaml
import websockets
from datetime import datetime, timezone


# ============================================================
# CONFIGURATION
# ============================================================

def load_config():
    with open("../../config/config.yml", "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


# ============================================================
# 1. BLUESKY CLIENT
# Responsabilité :
# Se connecter à Bluesky Jetstream et récupérer les événements
# ============================================================

class BlueskyClient:

    def __init__(self, jetstream_url):
        self.jetstream_url = jetstream_url

    async def connect(self):

        print("Connexion à Bluesky Jetstream...")

        websocket = await websockets.connect(
            self.jetstream_url
        )

        print("Connecté à Bluesky Jetstream !")

        return websocket


# ============================================================
# 2. BLUESKY EVENT TRANSFORMER
# Responsabilité :
# Transformer le JSON Bluesky en notre format standard
# ============================================================

class BlueskyEventTransformer:

    def transform(self, data):

        # On garde uniquement les commits
        if data.get("kind") != "commit":
            return None

        commit = data.get("commit", {})

        # On garde uniquement les créations
        if commit.get("operation") != "create":
            return None

        record = commit.get("record", {})

        # On garde uniquement les posts
        if record.get("$type") != "app.bsky.feed.post":
            return None

        text = record.get("text", "")

        if not text:
            return None

        # Construction de notre format JSON standardisé
        event = {
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

        return event

    def _extract_language(self, record):
        langs = record.get("langs", [])
        if langs:
            return langs[0]
        return None


# ============================================================
# 3. BLUESKY STREAM LISTENER
# Responsabilité :
# Ecouter continuellement Bluesky,
# normaliser les événements
# et les transmettre au reste du pipeline
# ============================================================

class BlueskyStreamListener:
    def __init__(self, client, transformer):

        self.client = client
        self.transformer = transformer

    async def start(self):
        websocket = await self.client.connect()
        print("En attente de nouveaux posts...")
        print()
        try:
            while True:

                # --------------------------------------------
                # Réception du message Bluesky
                # --------------------------------------------
                message = await websocket.recv()
                # JSON string -> dictionnaire Python
                data = json.loads(message)
                # --------------------------------------------
                # Normalisation
                # --------------------------------------------
                event = self.transformer.transform(data)
                # Événement non pertinent
                if event is None:
                    continue
                # --------------------------------------------
                # Diffusion de l'événement
                # --------------------------------------------
                await self.publish(event)

        finally:
            await websocket.close()

    async def publish(self, event):

        """
        Pour l'instant, notre "diffusion"
        consiste simplement à afficher le JSON.

        Plus tard cette méthode pourra envoyer
        l'événement vers Kafka.
        """

        print("=" * 70)
        print("NOUVEL EVENEMENT BLUESKY")
        print("=" * 70)

        print(
            json.dumps(
                event,
                ensure_ascii=False,
                indent=2
            )
        )

        print()


# ============================================================
# MAIN
# ============================================================

async def main():

    # Charger config.yml
    config = load_config()

    jetstream_url = config["bluesky"]["jetstream_url"]

    # Création des composants

    client = BlueskyClient(
        jetstream_url
    )

    transformer = BlueskyEventTransformer()

    listener = BlueskyStreamListener(
        client,
        transformer
    )

    # Démarrage du streaming

    await listener.start()


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print()
        print("Arrêt du programme.")