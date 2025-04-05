from confluent_kafka import Producer
import random
import time
import json
from datetime import datetime
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Configuration du Producer Kafka avec authentification SASL
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_USERNAME'),
    'sasl.password': os.getenv('KAFKA_PASSWORD'),
    'client.id': os.getenv('KAFKA_CLIENT_ID')
}

# Créer un Producteur Kafka avec la configuration
producer = Producer(conf)

# Fonction pour générer un ID de ticket
def generate_ticket_id():
    return random.randint(1000, 9999)

# Fonction pour générer une priorité aléatoire
def generate_priority():
    return random.choice(['Low', 'Medium', 'High'])

# Fonction pour produire un message Kafka
def produce_ticket():
    ticket = {
        'ticket_id': generate_ticket_id(),
        'client_id': random.randint(1, 100),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'request': 'Issue with the account login',
        'request_type': random.choice(['Technical', 'Billing', 'General Inquiry']),
        'priority': generate_priority()
    }
    
    ticket_json = json.dumps(ticket)
    producer.produce('client_tickets', value=ticket_json)
    producer.flush()
    print(f"Ticket produit: {ticket_json}")

# Nombre de tickets à produire
num_tickets = 10
count = 0

# Générer les tickets
while count < num_tickets:
    produce_ticket()
    time.sleep(2)
    count += 1
