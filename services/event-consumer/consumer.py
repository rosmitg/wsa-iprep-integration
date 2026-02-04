import pika
import json
import psycopg2
import time
print("🎧 Waiting for booking events...", flush=True)

RABBITMQ_HOST = "rabbitmq"
POSTGRES_HOST = "postgres"

def save_processed_event(data):
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database="airport_db",
        user="airport",
        password="airport"
    )

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_bookings (
            id SERIAL PRIMARY KEY,
            processed_data JSONB
        )
    """)

    cursor.execute(
        "INSERT INTO processed_bookings (processed_data) VALUES (%s)",
        [json.dumps(data)]
    )

    conn.commit()
    cursor.close()
    conn.close()

def callback(ch, method, properties, body):
    event = json.loads(body)
    print("📩 Received booking event:", event)

    # Simulate processing delay
    time.sleep(2)

    save_processed_event(event)
    print("✅ Event processed and stored")

def start_consumer():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST)
            )

            channel = connection.channel()
            channel.queue_declare(queue="booking_events")

            channel.basic_consume(
                queue="booking_events",
                on_message_callback=callback,
                auto_ack=True
            )

            print("🎧 Waiting for booking events...")
            channel.start_consuming()

        except Exception as e:
            print("Retrying connection...", e)
            time.sleep(5)

if __name__ == "__main__":
    start_consumer()
