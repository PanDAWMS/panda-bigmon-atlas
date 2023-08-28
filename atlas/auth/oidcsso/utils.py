import json
import logging

from atlas.settings import OIDC_CLIENT_ID
from atlas.settings.oidcclient import OIDC_SECRET

_logger = logging.getLogger('prodtaskwebui')



import requests

DEFAULT_SERVER = "auth.cern.ch"
DEFAULT_REALM = "cern"
DEFAULT_REALM_PREFIX = "auth/realms/{}"
DEFAULT_TOKEN_ENDPOINT = "api-access/token"
API_BASE_URL = "https://authorization-service-api.web.cern.ch/api/v1.0"

TARGET_API = "authorization-service-api"
def get_token_endpoint(server=DEFAULT_SERVER, realm=DEFAULT_REALM):
    """
    Gets the token enpdoint path from the args
    """
    return "https://{}/{}/{}".format(
        server, DEFAULT_REALM_PREFIX.format(realm), DEFAULT_TOKEN_ENDPOINT
    )


def get_api_token(client_id, client_secret, target_application, token_endpoint=get_token_endpoint()):
    _logger.debug(
        "[x] Getting API token as {} for {}".format(
            client_id, target_application
        )
    )

    response = requests.post(
        token_endpoint,
        auth=(client_id, client_secret),
        data={
            "grant_type": "client_credentials",
            "audience": target_application
        },
    )

    if not response.ok:
        _logger.error("ERROR getting token: {}".format(response.json()))
        exit(1)

    response_json = response.json()
    token = response_json["access_token"]

    print(token)
    return token


def get_roles(api_token):
    roles = requests.get(
        url=f"{API_BASE_URL}/Application/{OIDC_CLIENT_ID}/roles",
        headers={"Authorization": f"Bearer {api_token}"},
    )
    return roles

def set_role_group(api_token, role_group):
    response = set_role(api_token, role_group)
    if response.ok:
        role_id = response.json()['data']['id']
        return set_group(api_token, role_id, role_group)
    else:
        return response

def set_role(api_token, role_group):
    data = { 'name': role_group,
             'displayName': role_group,
             'description': role_group,
             'required': False,
             'multifactor': False,
             'applyToAllUsers': False}
    response = requests.post(
        url=f"{API_BASE_URL}/Application/{OIDC_CLIENT_ID}/roles",
        headers={"Authorization": f"Bearer {api_token}", 'Content-type': 'application/json'},

        data=json.dumps(data)
    )
    return response

def set_group(api_token, role_id, group):

    response = requests.post(
        url=f"{API_BASE_URL}/Application/{OIDC_CLIENT_ID}/roles/{role_id}/groups/{group}",
        headers={"Authorization": f"Bearer {api_token}", 'Content-type': 'application/json'},

    )
    return response

