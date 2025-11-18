import json
import os
import boto3
s3 = boto3.client('s3')
sns = boto3.client('sns')
def handler(event, context):
    try:
        resp = s3.list_objects_v2(Bucket=os.environ['CLEAN_BUCKET'], Prefix='archived/', MaxKeys=1000)
        count = resp.get('KeyCount', 0)
        metric = {'insight': 'recent_archive_count', 'value': count}
        sns.publish(TopicArn=os.environ['ALERT_TOPIC'], Message=json.dumps(metric), Subject='Daily Insights')
        return {'status': 'ok', 'count': count}
    except Exception as e:
        try:
            sns.publish(TopicArn=os.environ['ALERT_TOPIC'], Message='Insights job failed: ' + str(e), Subject='Insights Failure')
        except Exception:
            pass
        return {'status': 'error', 'error': str(e)}
