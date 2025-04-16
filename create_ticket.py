import uuid
from datetime import datetime
from random import randint, choice
import json
import time
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

# -----------------------------
# Fonction pour générer un ticket
# -----------------------------
def generate_ticket():
    demandes = ["Issue with the account login", "Forgot my password", "Other", "My computer burned"]
    priorities = ["High", "Medium", "Low"]
    demande_types = ["Technical", "Billing", "General Inquiry"]

    ticket = {}
    ticket["ticket_id"] = str(uuid.uuid4())
    ticket["client_id"] = str(randint(1,1000))
    now = datetime.now()
    ticket["demande"] = choice(demandes)
    ticket["demande_type"] = choice(demande_types)
    ticket["priority"] = choice(priorities)
    ticket["create_time"] = now.isoformat()

    return ticket

# -----------------------------
# Fonction pour créer un topic si nécessaire
# -----------------------------
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 3

def create_topic_if_not_exists(topic_name):
    admin_client = KafkaAdminClient(
        bootstrap_servers=["redpanda-0:9092", "redpanda-1:9092", "redpanda-2:9092"],
    )
    
    try:
        topic_list = []
        topic_list.append(NewTopic(
            name=topic_name,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        ))
        admin_client.create_topics(topic_list)
        print(f"Topic {topic_name} created successfully")
    except TopicAlreadyExistsError:
        print(f"Topic {topic_name} already exists")
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        admin_client.close()

# -----------------------------
# Fonction pour envoyer les messages
# -----------------------------
def send_messag(topic_name):
    producer = KafkaProducer(
        bootstrap_servers=["redpanda-0:9092", "redpanda-1:9092", "redpanda-2:9092"],
        value_serializer=lambda m: json.dumps(m).encode('ascii'),
        retries=5
    )
    print("Successfully connected to Redpanda cluster")

    create_topic_if_not_exists(topic_name)

    def on_success(metadata):
        print(f"Message produced to topic '{metadata.topic}' at offset {metadata.offset}")

    def on_error(e):
        print(f"Error sending message: {e}")

    try:
        while True:
            try:
                msg = generate_ticket()
                future = producer.send(topic_name, msg)
                future.add_callback(on_success)
                future.add_errback(on_error)
                producer.flush()
                time.sleep(1)
            except KeyboardInterrupt:
                print("Arrêt du producer...")
                break
            except Exception as e:
                print(f"Erreur: {e}")
                continue
    finally:
        if producer:
            producer.flush()
            producer.close()

# -----------------------------
# Point d'entrée du script
# -----------------------------
if __name__ == "__main__":
    time.sleep(10)  # Temps d'attente au démarrage
    topic_name = "client_tickets"
    send_messag(topic_name)
