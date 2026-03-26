import json
import time
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

SOURCE_FILE = Path("/opt/project/data/raw_sample/encounters.csv")
TOPIC = "encounter_events"
BOOTSTRAP_SERVERS = ["kafka:9092"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def main():
    df = pd.read_csv(SOURCE_FILE)
    sample_df = df.head(200)

    for _, row in sample_df.iterrows():
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "encounter_created",
            "event_ts": datetime.utcnow().isoformat(),
            "payload": row.fillna("").to_dict(),
        }
        producer.send(TOPIC, value=event)
        print(f"Sent encounter event: {event['event_id']}")
        time.sleep(0.2)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()