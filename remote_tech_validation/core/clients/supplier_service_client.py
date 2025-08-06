import base64
import requests
from aws_lambda_powertools import Logger

from remote_tech_validation.core.exceptions.profile_not_found import ProfileNotFound
from remote_tech_validation.core.exceptions.supplier_not_found import SupplierNotFound


class SupplierServiceClient:
    def __init__(
            self,
            auth_url: str,
            client_id: str,
            client_secret: str,
            api_key: str,
            base_url: str,
            _requests=requests
    ):
        self._auth_url = auth_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._api_key = api_key
        self._base_url = base_url
        self._requests = _requests
        self._logger = Logger()
        self._header = {
            "Authorization": f"Bearer {self._get_cognito_token()}",
            "Content-Type": "application/json",
            "x-api-key": self._api_key
        }

    # TODO
    # align logger with other services
    def get_supplier_info(self, supplier_id: str):
        supplier_endpoint = f"{self._base_url}/content/delivery/supplier?supplierId={supplier_id}"
        self._logger.info(f"GET Request Sent to {supplier_endpoint}")

        response = self._requests.get(supplier_endpoint, headers=self._header)
        if response.status_code == 404:
            raise SupplierNotFound(supplier_id)

        response.raise_for_status()
        resp_body = response.json()
        self._logger.info(f"Response Received: {resp_body}")

        return resp_body

    def get_content_profile(self, profile_id: str):
        profile_endpoint = f"{self._base_url}/content/delivery/profile?contentProfileId={profile_id}"
        self._logger.info(f"GET Request Sent to {profile_endpoint}")

        response = self._requests.get(profile_endpoint, headers=self._header)
        if response.status_code == 404:
            raise ProfileNotFound(profile_id)

        response.raise_for_status()
        resp_body = response.json()
        self._logger.info(f"Response Received: {resp_body}")

        return resp_body

    def _get_cognito_token(self):
        auth_string = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
        response = self._requests.post(
            url=self._auth_url,
            data="grant_type=client_credentials",
            headers={
                "Authorization": "Basic " + auth_string,
                "Content-Type": "application/x-www-form-urlencoded"
            }
        )
        self._logger.debug(response)
        response.raise_for_status()

        return response.json()['access_token']
