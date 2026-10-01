import io
import json
import os
import socket
import threading
import time
from kafka import KafkaConsumer, KafkaProducer
from PIL import Image, ImageFilter, ImageOps
import common

worker_id = f"{socket.gethostname()}-{os.getpid()}"
tiles_done = 0

filters = {
    "blur": lambda img: img.filter(ImageFilter.GaussianBlur(common.blur_radius)),
    "invert": ImageOps.invert,
}

def send_heartbeats(producer):
    while True:
        info = {"id": worker_id, "host": socket.gethostname(), "tiles": tiles_done}
        producer.send(common.heartbeat, json.dumps(info).encode())
        time.sleep(3)

def main():
    global tiles_done
    common.ensure_topics()
    producer = KafkaProducer(bootstrap_servers=common.server)
    threading.Thread(target=send_heartbeats, args=(producer,), daemon=True).start()

    # every worker uses the same group id, so kafka splits the partitions between them
    consumer = KafkaConsumer(
        common.tasks,
        bootstrap_servers=common.server,
        group_id="workers",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        # if a worker goes quiet for 10s kafka gives its partitions to the others
        session_timeout_ms=10000,
        heartbeat_interval_ms=3000,
    )
    print(f"{worker_id} waiting for tiles on {common.server}")

    try:
        for msg in consumer:
            meta = common.unpack(msg)
            img = Image.open(io.BytesIO(msg.value)).convert("RGB")
            img = filters[meta["filter"]](img).crop(meta["inner"])

            buf = io.BytesIO()
            img.save(buf, "PNG")
            meta["worker"] = worker_id
            producer.send(common.results, buf.getvalue(), headers=common.pack(meta))
            producer.flush()

            # commit only after the result is sent, if we crash the tile gets redelivered
            consumer.commit()
            tiles_done += 1
            print(f"job {meta['job']} tile {meta['idx']} ({meta['filter']}) done")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("bye")

if __name__ == "__main__":
    main()