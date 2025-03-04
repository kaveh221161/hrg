from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import os

app = Flask(name)

# دریافت آدرس Kafka از متغیر محیطی
KAFKA_BROKER = os.getenv("BROKER_URL", "localhost:9092")

# راه‌اندازی Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

@app.route('/send', methods=['POST'])
def send():
    data = request.json
    producer.send('my_topic', data)
    return jsonify({"message": "Data sent to Kafka!"})

if name == 'main':
    app.run(host='0.0.0.0', port=5000)
