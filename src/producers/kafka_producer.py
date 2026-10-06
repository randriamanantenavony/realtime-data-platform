import json
from confluent_kafka import Producer


class KafkaEventProducer:

    def __init__(self, bootstrap_servers, topic):

        self.topic = topic

        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "bluesky-ingestion"
        })

    def _delivery_report(self, err, msg):
        """
        Callback exécuté lorsque Kafka confirme
        ou refuse la livraison d'un événement.
        """

        if err is not None:

            print(
                f"Erreur Kafka : {err}"
            )

        else:

            print(
                f"Kafka OK -> "
                f"topic={msg.topic()} "
                f"partition={msg.partition()} "
                f"offset={msg.offset()}"
            )

    def send(self, event):
        """
        Envoie notre événement JSON normalisé vers Kafka.
        """

        message = json.dumps(
            event,
            ensure_ascii=False
        )

        self.producer.produce(
            topic=self.topic,
            value=message.encode("utf-8"),
            callback=self._delivery_report
        )

        # Permet d'exécuter les callbacks de livraison
        self.producer.poll(0)

    def close(self):
        """
        Attend que les derniers messages en attente
        soient réellement envoyés.
        """

        self.producer.flush()