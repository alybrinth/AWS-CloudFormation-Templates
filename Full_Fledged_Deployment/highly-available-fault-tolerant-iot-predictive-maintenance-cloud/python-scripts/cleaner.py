import json
import os
import boto3
import math

sqs = boto3.client("sqs")

def normalize_num(x):
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return round(v, 3)
    except Exception:
        return None

def handler(event, context):
    for r in event.get("Records", []):
        body = r.get("body", "")
        try:
            payload = json.loads(body)
        except Exception:
            payload = {"raw": body}

        cleaned = {}
        cleaned["deviceId"] = str(payload.get("deviceId", "unknown"))
        cleaned["timestamp"] = str(payload.get("timestamp", "0"))
        cleaned["status"] = str(payload.get("status", "unknown")).lower()

        cleaned["temp"] = normalize_num(payload.get("temp"))
        cleaned["voltage"] = normalize_num(payload.get("voltage"))

        if cleaned["temp"] is None and "temp" in payload:
            cleaned["temp"] = None
        if cleaned["voltage"] is None and "voltage" in payload:
            cleaned["voltage"] = None

        out = json.dumps(cleaned)

        sqs.send_message(
            QueueUrl=os.environ["PROCESS_QUEUE"],
            MessageBody=out
        )

    return {"status": "cleaned"}
