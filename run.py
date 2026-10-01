import os
import subprocess
import sys

def ask(prompt, default=""):
    suffix = f" [{default}]" if default else ""
    value = input(f"{prompt}{suffix}: ").strip()
    return value or default

def main():
    print("paraTile launcher")
    print("1. master")
    print("2. worker")

    choice = ask("Run", "1").lower()
    if choice in {"1", "master", "m"}:
        role = "master"
    elif choice in {"2", "worker", "w"}:
        role = "worker"
    else:
        raise SystemExit("Please choose 1/master or 2/worker.")

    kafka = ask("Kafka address", "localhost:9092")
    env = os.environ.copy()
    env["KAFKA"] = kafka

    if role == "master":
        remote_kafka = ask("Kafka address workers should use", kafka)
        host = ask("This machine's address for the UI", "")
        env["KAFKA_REMOTE"] = remote_kafka
        if host:
            env["PARATILE_HOST"] = host
        script = "master.py"
    else:
        script = "worker.py"

    print(f"Starting {script} with Kafka at {kafka}...")
    try:
        raise SystemExit(subprocess.run([sys.executable, script], env=env).returncode)
    except KeyboardInterrupt:
        print("\nStopped.")

if __name__ == "__main__":
    main()