import json
import os
from typing import Any

from aws_lambda_powertools import Logger

from remote_tech_validation.core.adapters.aws_adapter import AWSAdapter
from remote_tech_validation.core.adapters.media_info_adapter import MediaInfoAdapter
from remote_tech_validation.core.clients.supplier_service_client import SupplierServiceClient
from remote_tech_validation.core.exceptions.corrupted_file import CorruptedFile
from remote_tech_validation.core.exceptions.media_info_error import MediaInfoError
from remote_tech_validation.core.exceptions.parameter_store_error import ParameterStoreError
from remote_tech_validation.core.exceptions.profile_not_found import ProfileNotFound
from remote_tech_validation.core.exceptions.supplier_not_found import SupplierNotFound
from remote_tech_validation.core.metrics_logger import publish_cloudwatch_metric
from remote_tech_validation.core.profile_matcher import ProfileMatcher
from remote_tech_validation.core.skip_full_valdation import SkipValidator
from remote_tech_validation.core.url_checker import _check_if_target_url_exists

logger = Logger()


def lambda_handler(event: dict, context: Any) -> dict:
    """Handler for running mediainfo
    :param event: AWS APIGW Event
    :type event: dict
    :param context: AWS APIGW Context
    :type context: Any
    :raises e: Dynamodb Client Error
    :return: Transfer Request Item
    :rtype: str
    """

    logger.info('Event parameter: {}'.format(event))
    logger.info(f"Payload: {event['body']}")

    payload = json.loads(event['body'])
    media_id = payload.get('assetId')
    filepath = payload.get('filepath')

    aws_adapter = AWSAdapter(filepath=filepath)

    if SkipValidator.should_skip(filepath):
        logger.info(f"Skipping validation for extension of filepath: {filepath}")
        url_list = _get_url_list_from_target_url(payload)
        target_url_check_response = _check_if_target_url_exists(aws_adapter, url_list)


        if target_url_check_response:
            return target_url_check_response

        publish_cloudwatch_metric("SuccessfulChecks", media_id, "RTV is completed with mediaInfo verification skipped")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "RTV is completed with mediaInfo verification skipped"
            }),
        }

    # check bucket permission (of different region)
    if not aws_adapter.can_access_s3_access():
        publish_cloudwatch_metric("FailedChecks", media_id, "Error accessing S3 bucket")
        cant_access_bucket_response = {
            "statusCode": 500,
            "body": json.dumps({
                "status": "fail",
                "errorMessage": "Error accessing S3 bucket"
            }),
        }
        logger.info(f"RETURNING WITH {cant_access_bucket_response}")
        return cant_access_bucket_response

    # validate file existence (filepath from request)
    if not aws_adapter.check_s3_file_exists():
        publish_cloudwatch_metric("FailedChecks", media_id, "File not found")
        file_not_found_response = {
            "statusCode": 404,
            "body": json.dumps({
                "status": "fail",
                "errorMessage": "Request filepath: File not found"
            }),
        }
        logger.info(f"RETURNING WITH {file_not_found_response}")
        return file_not_found_response

    try:
        supplier_service_client = SupplierServiceClient(
            auth_url=aws_adapter.get_value_from_parameter_store(os.environ.get("SUPPLIER_AUTH_ENDPOINT")),
            client_id=aws_adapter.get_value_from_parameter_store(os.environ.get("SUPPLIER_AUTH_CLIENT_ID")),
            client_secret=aws_adapter.get_value_from_parameter_store(os.environ.get("SUPPLIER_AUTH_CLIENT_SECRET")),
            api_key=aws_adapter.get_value_from_parameter_store(os.environ.get("SUPPLIER_API_KEY")),
            base_url=aws_adapter.get_value_from_parameter_store(os.environ.get("SUPPLIER_API_URL"))
        )

        supplier_id = payload.get('supplierId')
        supplier_info = supplier_service_client.get_supplier_info(supplier_id)

        content_profiles_list = []
        profile_not_found_list = []
        for profile in supplier_info['contentProfile']:
            try:
                profile_info = supplier_service_client.get_content_profile(profile)
                # logger.debug(profile_info)
                content_profiles_list.append(profile_info)
            except ProfileNotFound as e:
                logger.warning(f"(At least one) Profile not found: {e.message}")
                profile_not_found_list.append(profile)

        if len(content_profiles_list) == 0:  # should have at least 1 profile to continue
            if len(profile_not_found_list) == 0:
                logger.error(f"No profile defined for the supplier {supplier_id}")
                raise ProfileNotFound("None")
            else:
                id_list = ", ".join(profile_not_found_list)
                logger.error(f"None of the profile(s) found for the supplier {supplier_id} : {id_list}")
                raise ProfileNotFound(id_list)
        elif len(profile_not_found_list) != 0:
            logger.warning("Failed to get some profile(s), but will continue with the rest")

        # we now have a list of content profiles for this supplier
        logger.info('Content profiles:')
        logger.info(json.dumps(content_profiles_list))

        media_info_adapter = MediaInfoAdapter(aws_adapter)
        media_profile = media_info_adapter.build_profile_from_mediainfo()

        # check the media profile matches one of the supplier content profiles
        match = ProfileMatcher().is_media_matching_with_any_profiles(content_profiles_list, media_profile)
        if not match:
            logger.info('no match between supplier profiles and mediaInfo output')
            publish_cloudwatch_metric("FailedChecks", media_id, "File not at specs")
            return {
                "statusCode": 409,
                "body": json.dumps({
                    "errorMessage": "File not at specs"
                }),
            }
    except ParameterStoreError:
        publish_cloudwatch_metric("FailedChecks", media_id, "Failed to get param store values")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Failed to get param store values"}),
        }
    except (SupplierNotFound, ProfileNotFound, CorruptedFile) as e:
        publish_cloudwatch_metric("FailedChecks", media_id, e.message)
        error_response = {
            "statusCode": 415,
            "body": json.dumps({
                "status": "fail",
                "errorMessage": e.message
            }),
        }
        logger.info(f"RETURNING WITH {error_response}")
        return error_response
    except FileNotFoundError:
        publish_cloudwatch_metric("FailedChecks", media_id, "File not found")
        cant_access_bucket_response = {
            "statusCode": 404,
            "body": json.dumps({
                "status": "fail",
                "errorMessage": "File not found"
            }),
        }
        logger.info(f"RETURNING WITH {cant_access_bucket_response}")
        return cant_access_bucket_response
    except MediaInfoError as e:
        publish_cloudwatch_metric("FailedChecks", media_id, f"Could not run MediaInfo: {e}")
        media_info_error_response = {
            "statusCode": 500,
            "body": json.dumps({
                "status": "fail",
                "errorMessage": f"Could not run MediaInfo: {e}"
            }),
        }
        logger.info(f"RETURNING WITH {media_info_error_response}")
        return media_info_error_response
    except Exception as e:
        error_response = {
            "statusCode": 500,
            "body": json.dumps({
                "status": "fail",
                "errorMessage": f"Unexpected {type(e).__name__} error: {e}"
            }),
        }
        logger.info(f"RETURNING WITH {error_response}")
        return error_response

    url_list = _get_url_list_from_target_url(payload)
    target_url_check_response = _check_if_target_url_exists(aws_adapter, url_list)


    if target_url_check_response:
        return target_url_check_response

    publish_cloudwatch_metric("SuccessfulChecks", media_id, "Remote Tech Validation completed with no issue")
    # Finish the Lambda function with an HTTP 200 status code:
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Remote Tech Validation completed with no issue"
        }),
    }


def _get_url_list_from_target_url(payload: dict) -> list:
    url_list = []
    target_url = payload.get("targetUrl")
    logger.info(f"targetUrl: {target_url}")
    if target_url:
        target_url_filename = _get_full_path(target_url, payload.get("filename"))  # filename suppose to be assetId.mxf
        url_list.append(target_url_filename)
        logger.info(f"Added targetUrl: {target_url_filename}")
        file_ext = _get_file_extension(payload.get("filename"))
        if file_ext:
            target_url_asset_id = _get_full_path(target_url, f"{payload.get("assetId")}{file_ext}")
            if target_url_filename != target_url_asset_id:
                url_list.append(target_url_asset_id)
                logger.info(f"Added also targetUrl: {target_url_asset_id}")
    return url_list


def _get_full_path(target_url: str, filename: str) -> str:
    if target_url.endswith('/'):
        return f"{target_url}{filename}"
    else:
        return f"{target_url}/{filename}"


def _get_file_extension(filename: str) -> str:
    parts = filename.split(".")
    return f".{parts[-1]}" if len(parts) > 1 else None
