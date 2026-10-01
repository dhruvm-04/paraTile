import io
import json
import math
import os
import socket
import threading
import time
import uuid
from collections import Counter
from contextlib import asynccontextmanager
from pathlib import Path
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from PIL import Image, ImageOps, UnidentifiedImageError
import common

tile_size = 200
max_side = 1200
filter_names = ["blur", "invert"]

jobs = {}
workers = {}
producer = None

def lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()

def send_tiles(job_id, img, filter_name):
    w, h = img.size
    n_parts = len(producer.partitions_for(common.tasks))
    idx = 0
    for y in range(0, h, tile_size):
        for x in range(0, w, tile_size):
            x2, y2 = min(x + tile_size, w), min(y + tile_size, h)
            # cut a bit more than the tile so the filter has context around it
            box = (max(x - common.pad, 0), max(y - common.pad, 0),
                   min(x2 + common.pad, w), min(y2 + common.pad, h))
            buf = io.BytesIO()
            img.crop(box).save(buf, "PNG")
            meta = {
                "job": job_id, "idx": idx, "x": x, "y": y, "filter": filter_name,
                "inner": [x - box[0], y - box[1], x2 - box[0], y2 - box[1]],
            }
            # spread tiles over the partitions, kafka hands partitions to workers
            producer.send(common.tasks, buf.getvalue(), partition=idx % n_parts,
                          headers=common.pack(meta))
            idx += 1
    producer.flush()

def stitch(job):
    out = Image.new("RGB", job["size"])
    for meta, data in job["tiles"].values():
        out.paste(Image.open(io.BytesIO(data)), (meta["x"], meta["y"]))
    buf = io.BytesIO()
    out.save(buf, "PNG")
    job["image"] = buf.getvalue()

def collect_results():
    consumer = KafkaConsumer(common.results, bootstrap_servers=common.server,
                             group_id="master", auto_offset_reset="latest")
    for msg in consumer:
        meta = common.unpack(msg)
        job = jobs.get(meta["job"])
        # skip old jobs and duplicates (kafka can redeliver a tile)
        if job is None or meta["idx"] in job["tiles"]:
            continue
        job["tiles"][meta["idx"]] = (meta, msg.value)
        job["by_worker"][meta["worker"]] += 1
        if len(job["tiles"]) == job["total"]:
            stitch(job)

def watch_heartbeats():
    consumer = KafkaConsumer(bootstrap_servers=common.server, enable_auto_commit=False)
    # heartbeat topic only has one partition
    consumer.assign([TopicPartition(common.heartbeat, 0)])
    consumer.seek_to_end()
    for msg in consumer:
        info = json.loads(msg.value)
        info["seen"] = time.time()
        workers[info["id"]] = info

@asynccontextmanager
async def lifespan(app):
    global producer
    common.ensure_topics()
    producer = KafkaProducer(bootstrap_servers=common.server)
    threading.Thread(target=collect_results, daemon=True).start()
    threading.Thread(target=watch_heartbeats, daemon=True).start()
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "index.html")

@app.get("/api/info")
def info():
    ip = os.getenv("PARATILE_HOST", lan_ip())
    return {
        "ip": ip,
        "kafka": common.server,
        "remote_kafka": common.remote_server,
        "partitions": len(producer.partitions_for(common.tasks)),
        "topics": [common.tasks, common.results, common.heartbeat],
    }

@app.get("/api/workers")
def get_workers():
    now = time.time()
    live = [w for w in workers.values() if now - w["seen"] < 10]
    live.sort(key=lambda w: w["id"])
    return {"count": len(live), "workers": live}

@app.post("/api/process")
async def process(request: Request, filter: str):
    if filter not in filter_names:
        raise HTTPException(400, "filter must be blur or invert")
    try:
        img = Image.open(io.BytesIO(await request.body()))
        img = ImageOps.exif_transpose(img).convert("RGB")
    except (UnidentifiedImageError, OSError):
        raise HTTPException(400, "that doesnt look like an image")
    img.thumbnail((max_side, max_side))

    job_id = uuid.uuid4().hex[:8]
    total = math.ceil(img.width / tile_size) * math.ceil(img.height / tile_size)
    jobs[job_id] = {"total": total, "size": img.size, "tiles": {},
                    "by_worker": Counter(), "image": None}
    # dont keep every job forever
    while len(jobs) > 20:
        jobs.pop(next(iter(jobs)))

    send_tiles(job_id, img, filter)
    return {"job": job_id, "total": total}

@app.get("/api/job/{job_id}")
def job_status(job_id: str):
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(404, "unknown job")
    return {"done": len(job["tiles"]), "total": job["total"],
            "finished": job["image"] is not None, "by_worker": job["by_worker"]}

@app.get("/api/job/{job_id}/image")
def job_image(job_id: str):
    job = jobs.get(job_id)
    if job is None or job["image"] is None:
        raise HTTPException(404, "not ready")
    return Response(job["image"], media_type="image/png")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)