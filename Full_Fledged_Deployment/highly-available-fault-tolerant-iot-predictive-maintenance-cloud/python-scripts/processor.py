import json
import os
import boto3
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')
sns = boto3.client('sns')
def handler(event, context):
    for rec in event.get('Records', []):
        body = rec.get('body', '')
        try:
            payload = json.loads(body)
        except Exception:
            payload = {'raw': body}
        device_id = payload.get('deviceId') or payload.get('device_id') or 'unknown'
        ts = payload.get('timestamp') or payload.get('ts') or context.aws_request_id
        status = payload.get('status', 'ok')
        temp = payload.get('temp', 0)
        item = {
            'deviceId': {'S': str(device_id)},
            'timestamp': {'S': str(ts)},
            'status': {'S': str(status)}
        }
        try:
            if status == 'failed' or (isinstance(temp, (int, float)) and temp > 100):
                sns.publish(TopicArn=os.environ['ALERT_TOPIC'], Message=json.dumps({'device': device_id, 'reason': 'detected failure', 'payload': payload}), Subject='IoT Device Failure Detected')
        except Exception:
            pass
        try:
            dynamodb.put_item(TableName=os.environ['DDB_TABLE'], Item=item)
        except Exception:
            pass
        try:
            s3.put_object(Bucket=os.environ['CLEAN_BUCKET'], Key=f"archived/{device_id}/{ts}.json", Body=json.dumps(payload))
        except Exception:
            pass
    return {'status': 'ok'}
