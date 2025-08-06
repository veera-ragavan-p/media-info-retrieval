import json

from botocore.exceptions import ClientError

from remote_tech_validation.core.adapters.aws_adapter import AWSAdapter


def _check_if_target_url_exists(aws_adapter: AWSAdapter, url_list: list) -> dict | None:
    """
    Check if any of the given S3 URLs already exist.

    :param aws_adapter: AWSAdapter instance
    :param url_list: List of S3 URLs to check
    :return: Response dict if a URL exists or an error occurs, else None
    """
    try:
        for url in url_list:
            if aws_adapter.check_s3_url_file_exists(url):
                return {
                    "statusCode": 415,
                    "body": json.dumps({
                        "status": "fail",
                        "errorMessage": "Request targetUrl: Target file already exists"
                    }),
                }
    except ClientError as ce:
        if (ce.response.get('Error', {}).get('Code') is not None
                and ce.response.get('Error', {}).get('Message') is not None):
            error_msg = f"{ce.response['Error']['Code']}:{ce.response['Error']['Message']}"
        else:
            error_msg = "Unknown error"
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "fail",
                "errorMessage": f"Error checking if file exists - {error_msg}"
            }),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "fail",
                "errorMessage": f"Error checking if file exists - Unexpected {type(e).__name__} error"
            }),
        }

    return None


