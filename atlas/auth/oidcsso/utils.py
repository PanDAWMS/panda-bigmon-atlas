import json
import logging

from django.contrib.auth.models import User, Group

from atlas.prodtask.models import IAM_USER
from atlas.settings import OIDC_CLIENT_ID
from atlas.settings import IAM
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


def get_iam_token():
    _logger.debug(
        "[x] Getting API token as {} ".format(
            IAM['client_id']
        )
    )

    response = requests.post(
        f'{IAM["api_base"]}/token',
        auth=(IAM['client_id'], IAM['secret']),
        data={
            "grant_type": "client_credentials",
        },
    )

    if not response.ok:
        _logger.error("ERROR getting token: {}".format(response.json()))

    response_json = response.json()
    token = response_json["access_token"]

    return token

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

def get_user_from_iam(api_token, username):
    user = requests.get(
        url=f"{IAM['api_base']}/scim/users?filter=displayName eq '{username}'",
        headers={"Authorization": f"Bearer {api_token}"},
    )
    return user

def read_all_users(token, api_url=f"{IAM['api_base']}/scim/Users", max_page_size=1000):
    headers = {
        'Authorization': f'Bearer {token}'
    }
    users = []
    startIndex = 1
    totalResults = None

    while totalResults is None or startIndex < totalResults:
        params = {
            'attributes': 'userName,groups,name',
            'startIndex': startIndex,
            'count': max_page_size
        }
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()  # This will raise an exception for HTTP error codes
        data = response.json()

        users.extend(data.get('Resources', []))
        itemsPerPage = data.get('itemsPerPage', max_page_size)
        totalResults = data.get('totalResults', 0)
        startIndex += itemsPerPage

    return users

def read_iam_user(token, user_id, api_url=f"{IAM['api_base']}/scim/Users"):
    headers = {
        'Authorization': f'Bearer {token}'
    }
    response = requests.get(f"{api_url}/{user_id}", headers=headers)
    response.raise_for_status()  # This will raise an exception for HTTP error codes
    return response.json()


def filter_iam_group(group_name) -> str|None:
    if group_name.startswith('atlas/') and group_name.endswith('production') and '-' in group_name.split('/')[1]:
        return f'IAM:{group_name}'
    return None

def update_groups_for_user(user: User, iam_groups):
    for group in iam_groups:
        filtered_group = filter_iam_group(group['display'])
        if filtered_group is not None:
            django_group, new_group = Group.objects.get_or_create(name=filtered_group)
            if django_group not in user.groups.all():
                _logger.info(f'Adding user {user.username} to group {filtered_group}')
                user.groups.add(django_group)
    user.save()


def fill_user_groups_from_iam(fill_new=True):
    token = get_iam_token()
    iam_users = read_all_users(token)
    iam_users_dict = {}
    for user in iam_users:
        iam_users_dict[user['userName']] = user
    current_users = User.objects.all()
    for user in current_users:
        update_groups = False
        if user.username in iam_users_dict:
            if not IAM_USER.objects.filter(username=user.username).exists():
                iam_user = IAM_USER(username=user.username, userID=iam_users_dict[user.username]['id'])
                iam_user.save()
                update_groups = True
            if update_groups or not fill_new:
                update_groups_for_user(user, iam_users_dict[user.username].get('groups',[]))
        else:
            if not IAM_USER.objects.filter(username=user.username).exists():
                iam_user = IAM_USER(username=user.username, userID='NOT_FOUND')
                iam_user.save()

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

