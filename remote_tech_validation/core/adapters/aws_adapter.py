import os
from urllib.parse import urlparse

import boto3
from aws_lambda_powertools import Logger
from botocore.config import Config
from botocore.exceptions import ClientError

from remote_tech_validation.core.exceptions.parameter_store_error import ParameterStoreError


class AWSAdapter:
    def __init__(self, filepath: str, boto_client=boto3):
        self._filepath = filepath
        self._boto_client = boto_client
        self._logger = Logger()
        self._initialise_properties()

    def _initialise_properties(self):
        self._parse_s3_url(self._filepath)
        self._get_aws_region_from_bucket()

    def get_value_from_parameter_store(self, parameter_name: str) -> str:
        try:
            self._logger.info(f"Getting value from parameter store for '{parameter_name}'")
            ssm = self._boto_client.client("ssm")
            parameter_response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
            return parameter_response["Parameter"]["Value"]
        except Exception:
            self._logger.error(f"Failed to get param store values for {parameter_name}")
            raise ParameterStoreError(parameter_name)

    # TODO should we write unit test for existing functionality?
    def get_signed_url_for_asset(self):
        s3_cli = self._boto_client.client(
            "s3",
            region_name=self._region,
            config=Config(signature_version='s3v4', s3={'addressing_style': 'virtual'})
        )

        self._logger.info(f"GETTING SIGNED URL FOR {self._bucket_name} BUCKET AND {self._object_key} FILE")
        signed_url = s3_cli.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._bucket_name, "Key": self._object_key},
            ExpiresIn=300
        )
        self._logger.debug(f"SIGNED URL: {signed_url}")
        return signed_url

    def can_access_s3_access(self) -> bool:
        self._logger.info(f"Lambda trying to access bucket: {self._bucket_name} in {self._region} region")
        s3 = self._boto_client.client('s3', region_name=self._region)

        try:
            s3.head_bucket(Bucket=self._bucket_name)
            self._logger.info(f"Verified access")
            return True
        except Exception as e:
            self._logger.error(f"Failed to access {self._bucket_name} in {self._region}: {str(e)}")
            return False

    def check_s3_url_file_exists(self, url=None) -> bool:
        return self.check_s3_file_exists(url)

    def check_s3_file_exists(self, url=None) -> bool:
        self._logger.info(f"Checking if file exists in S3: {url}")
        if url:
            self._parse_s3_url(url)
        self._logger.info(f"Checking if file exists in bucket: {self._bucket_name} with key: {self._object_key}")
        s3_client = self._boto_client.client('s3', region_name=self._region)

        try:
            s3_client.head_object(Bucket=self._bucket_name, Key=self._object_key)
            self._logger.info("File exists")
            return True
        except ClientError as e:
            if e.response.get('Error', {}).get('Code', None) == '404':
                self._logger.info(f"File not found: {self._object_key}")
                return False
            else:
                self._logger.error(f"Error checking if file exists: {e}", exc_info=True)
                raise

    def _get_aws_region_from_bucket(self) -> None:
        env = os.environ.get("ENVIRONMENT")
        bucket_region_map = {
            f"{env}-cntdel-euc1-de-gap-cd-s3-bucket": "eu-central-1",
            f"{env}-cntdel-euc1-de-gap-de-master-s3-bucket": "eu-central-1",
            f"{env}-cntdel-skymaster-s3": "eu-west-2",
            f"{env}-cntdel-gap-access-service-s3": "eu-west-2",
            f"{env}-cntdel-gap-awm-s3": "eu-west-2",
            f"{env}-cntdel-gap-cd-s3": "eu-west-2",
            f"{env}-cntdel-gap-commercials-s3": "eu-west-2"
        }
        self._region = bucket_region_map.get(self._bucket_name, os.environ.get('REGION'))

    def _parse_s3_url(self, s3_url: str) -> None:
        parsed_url = urlparse(s3_url)
        bucket_name = parsed_url.netloc  # The bucket name
        object_key = parsed_url.path.lstrip("/")  # Remove leading slash to get object key
        self._bucket_name = bucket_name
        self._object_key = object_key
