from typing import Dict, Any

import requests

from multi_data_manager.core.exceptions import APIError
from multi_data_manager.core.logger import logger


class APIHandler:
    """
    Handler for API interactions.
    """

    @staticmethod
    def invoke_api(api_url: str, auth_token: str) -> Dict[str, Any]:
        """
        Invokes an API endpoint with the provided authentication token.

        Args:
            api_url (str): The URL of the API endpoint.
            auth_token (str): The authentication token for the API.

        Returns:
            Dict[str, Any]: The JSON response from the API.
        """
        try:
            headers = {
                "Authorization": auth_token,
                "Accept": "application/json",
                "Content-Type": "application/json"
            }

            with requests.Session() as session:
                session.trust_env = False
                response = session.get(api_url, headers=headers, verify=True)

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f'Error occurred while invoking API: {e}')
            raise APIError(f"API request failed: {e}")
        except Exception as e:
            logger.error(f'Unknown error occurred in invoke_api() function: {e}')
            raise APIError(f"Unknown API error: {e}")
