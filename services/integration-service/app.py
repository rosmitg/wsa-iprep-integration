from fastapi import FastAPI
import pika
import json
import psycopg2

app = FastAPI()

RABBITMQ_HOST = "rabbitmq"
POSTGRES_HOST = "postgres"

def publish_event(message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    channel.queue_declare(queue="booking_events")

    channel.basic_publish(
        exchange="",
        routing_key="booking_events",
        body=json.dumps(message)
    )

    connection.close()

def save_booking(data):
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database="airport_db",
        user="airport",
        password="airport"
    )

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            id SERIAL PRIMARY KEY,
            booking_data JSONB
        )
    """)

    cursor.execute(
        "INSERT INTO bookings (booking_data) VALUES (%s)",
        [json.dumps(data)]
    )

    conn.commit()
    cursor.close()
    conn.close()

@app.get("/health")
def health():
    return {"status": "integration service running"}

@app.post("/booking")
def receive_booking(data: dict):
    save_booking(data)
    publish_event(data)
    return {"status": "booking received"}
