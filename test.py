import json
import logging 
import os

from remote_tech_validation.core.adapters.aws_adapter import AWSAdapter
from remote_tech_validation.core.adapters.media_info_adapter import MediaInfoAdapter


logger = logging.getLogger(__name__)

def lambda_handler(event: dict, context: dict) -> dict:
    logger.info("Event: %s", event)
    # Your processing logic here


    payload = json.loads(event.get("body", "{}"))
    filepath = payload.get("filepath")

    aws_adapter = AWSAdapter(filepath=filepath)
    media_info_adapter = MediaInfoAdapter(aws_adapter)
    media_profile = media_info_adapter.build_profile_from_mediainfo()



    return {"statusCode": 200, "body": media_profile}


def set_env_variables() -> None:
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["REGION"] = "eu-west-2"

if __name__ == "__main__":
    set_env_variables()
    lambda_handler({"body": json.dumps({"filepath": "s3://dev-cntdel-euc1-de-gap-cd-s3-bucket/source/VE/PAV7996864/PAV7996864.mxf4"})}, None)
