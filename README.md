# paraTile

paraTile splits an image into tiles, processes them in parallel, and stitches the
result back together. Kafka connects the master and workers.

The master runs the web page and coordinates the work. Workers can run on the same
computer or on other computers, and you can start as many as Kafka's task partitions
allow.

## Files

- `master.py` - web server and image coordinator
- `worker.py` - image worker
- `common.py` - Kafka settings and topics
- `run.py` - interactive launcher

## Network setup

For workers on other computers, put all machines on the same ZeroTier network. Join
and authorize each machine in ZeroTier Central, then use the master's ZeroTier IP.

Kafka must advertise an address workers can reach:

```properties
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://<master-zerotier-ip>:9092
```

Allow TCP ports `8000` and `9092` through the master's firewall. Do not expose Kafka
to the public internet.

## Run

Start Kafka first, then install the Python packages:

```powershell
python -m pip install -r requirements.txt
```

Start the launcher:

```powershell
python run.py
```

Choose `master` on the master computer and enter its Kafka and ZeroTier addresses.
Choose `worker` on each worker computer and enter `<master-zerotier-ip>:9092`.

Open `http://<master-zerotier-ip>:8000` in a browser on an authorized machine.
