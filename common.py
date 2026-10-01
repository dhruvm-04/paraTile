import json
import os
import sys
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

# Kafka address used by this process
server = os.getenv("KAFKA", "localhost:9092")
remote_server = os.getenv("KAFKA_REMOTE", server)

tasks = "tasks"
results = "results"
heartbeat = "heartbeat"

# partitions on the tasks topic = max workers that can run at the same time
partitions = int(os.getenv("PARTITIONS", "12"))

blur_radius = 6
# extra pixels around every tile so the blur has no seams at the edges
pad = blur_radius * 3

def ensure_topics():
    try:
        admin = KafkaAdminClient(bootstrap_servers=server)
    except KafkaError:
        sys.exit(f"cant reach kafka at {server}, is it running?")
    for name, count in [(tasks, partitions), (results, 1), (heartbeat, 1)]:
        try:
            admin.create_topics([NewTopic(name, count, 1)])
        except TopicAlreadyExistsError:
            pass
    admin.close()

# tile info travels in a kafka header, the image bytes are the message value
def pack(meta):
    return [("meta", json.dumps(meta).encode())]

def unpack(msg):
    return json.loads(dict(msg.headers)["meta"])