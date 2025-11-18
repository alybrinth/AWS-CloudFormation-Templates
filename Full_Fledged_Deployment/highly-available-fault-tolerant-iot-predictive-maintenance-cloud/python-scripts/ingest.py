import json
import os
import boto3
import base64
sqs = boto3.client('sqs')

def handler(event, context):
    body = event.get("body", "")
    if event.get("isBase64Encoded"):
        try:
            body = base64.b64decode(body).decode("utf-8")
        except Exception:
            pass

    try:
        payload = json.loads(body)
    except Exception:
        payload = {"raw": body}

    device_id = payload.get("deviceId") or payload.get("device_id") or "unknown"
    ts = payload.get("timestamp") or payload.get("ts") or context.aws_request_id

    minimal = {
        "deviceId": device_id,
        "timestamp": ts,
        "status": payload.get("status", "unknown"),
        "temp": payload.get("temp", None),
        "voltage": payload.get("voltage", None),
        "raw": payload
    }

    sqs.send_message(
        QueueUrl=os.environ["PREPROCESS_QUEUE"],
        MessageBody=json.dumps(minimal)
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"accepted": True, "deviceId": device_id})
    }
