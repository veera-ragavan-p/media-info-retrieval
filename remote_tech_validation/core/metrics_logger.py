import boto3
import json
import time
import os
import botocore.exceptions
from aws_lambda_powertools import Logger

logger = Logger()
ENV = os.environ.get("ENVIRONMENT", "dev")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-2")

logs = boto3.client("logs", region_name=AWS_REGION)

# CloudWatch Log Group
LOG_GROUP_NAME = f"/aws/lambda/{ENV}-cntdel-euw2-gap-remote-tech-validation-main"

def get_latest_log_stream():
    """Fetches the latest log stream name for the log group or creates a new one."""
    try:
        response = logs.describe_log_streams(
            logGroupName=LOG_GROUP_NAME,
            orderBy='LastEventTime',
            descending=True,
            limit=1
        )
        log_streams = response.get("logStreams", [])

        if log_streams:
            log_stream_name = log_streams[0].get("logStreamName")
            logger.info(f"Using existing log stream: {log_stream_name}")
            return log_stream_name
        else:
            logger.warning("No existing log stream found. Creating a new one.")
            return create_new_log_stream()

    except botocore.exceptions.ClientError as e:
        if "AccessDeniedException" in str(e):
            logger.error(f"Access denied when fetching log streams: {e}")
        else:
            logger.error(f"Failed to describe log streams: {e}")

        return create_new_log_stream()  # Always return a log stream name


def create_new_log_stream():
    """Creates a new log stream in CloudWatch Logs and returns its name."""
    new_log_stream_name = f"media-checks-{int(time.time())}"
    try:
        logs.create_log_stream(logGroupName=LOG_GROUP_NAME, logStreamName=new_log_stream_name)
        logger.info(f"Created new log stream: {new_log_stream_name}")
        return new_log_stream_name
    except botocore.exceptions.ClientError as e:
        logger.error(f"Failed to create new log stream: {e}")
        return None  # Return None to indicate failure

def publish_cloudwatch_metric(metric_name: str, media_id: str, message: str = None):
    """
    Publishes a log in CloudWatch Logs using Embedded Metric Format (EMF).
    CloudWatch will extract the metrics automatically.

    :param metric_name: "SuccessfulChecks" or "FailedChecks"
    :param media_id: The media ID being checked
    :param message: Message to include in the log event
    """
    try:
        timestamp = int(time.time() * 1000)

        log_event = {
            "_aws": {
                "Timestamp": timestamp,
                "CloudWatchMetrics": [
                    {
                        "Namespace": "RTV-MediaChecks",
                        "Metrics": [{"Name": metric_name, "Unit": "Count"}],
                        "Dimensions": [["MediaID"]],
                    }
                ],
            },
            "MediaID": media_id,
            metric_name: 1,
        }

        if metric_name == "FailedChecks" and message:
            log_event["FailureReason"] = message
            logger.warning(f"Media check failed: {message}")


        # Ensure we get a single log stream name
        log_streams = logs.describe_log_streams(
            logGroupName=LOG_GROUP_NAME,
            orderBy='LastEventTime',
            descending=True,
            limit=1
        ).get("logStreams", [])

        if not log_streams:
            logger.warning("No log streams found, creating a new one.")
            log_stream = create_new_log_stream()
        else:
            log_stream = log_streams[0].get("logStreamName")

        if not log_stream:
            logger.error("No valid log stream available. Skipping log event.")
            return

        log_message = json.dumps(log_event)
        logger.info(f"CloudWatch Log Event: {log_message}")

        logs.put_log_events(
            logGroupName=LOG_GROUP_NAME,
            logStreamName=log_stream,
            logEvents=[
                {
                    "timestamp": timestamp,
                    "message": log_message
                }
            ]
        )

        logger.info(f"Successfully logged to CloudWatch Logs (EMF): {log_event}")

    except botocore.exceptions.ClientError as e:
        logger.error(f"Client error while logging to CloudWatch: {e}")

    except Exception as e:
        logger.error(f"Unexpected error while logging to CloudWatch: {e}")