"""Deployment configuration, independent of Django and external services.

Environment mode is opt-in with PRODSYS_CONFIG_MODE=environment or
ATLAS_CONFIG_DIR. With neither set, settings/runtime.py uses legacy local.py.
"""
import copy
import json
import os
from pathlib import Path
import re
import shlex


class ConfigurationError(ValueError):
    """Invalid deployment configuration (messages never include values)."""


# Existing names are deliberately retained while callers migrate from local.py.
STRING_SETTINGS = '''MY_SECRET_KEY IDDS_MESSAGING_PASSWORD JIRA_KEY LOG_ROOT MY_CELERY
PANDA_DB_SCHEMA DEFT_DB_SCHEMA JEDI_BASE_URL CERTIFICATE_PATH CERT_PEM_PATH
CERT_KEY_PEM_PATH DEFT_AUTH_USER DEFT_AUTH_KEY FAKE_LOGIN_AS_USER OPENAI_KEY
OIDC_SECRET BASE_DEFT_API_URL BIGPANDA_TOKEN EMAIL_HOST CELERY_CONNECTION
VOMS_PROXY_CERT LOG_MODE ORACLE_CONFIG_DIR AMI_API_V2_BASE_URL
AMI_API_V2_BASE_URL_REPLICA CRIC_BASE_URL JIRA_AUTH_URL JIRA_ISSUE_URL
IDDS_MESSAGING_HOST IDDS_MESSAGING_USERNAME IDDS_MESSAGING_QUEUE RUCIO_ACCOUNT
OIDC_CLIENT_ID URL_PATH_PREFIX STATIC_ROOT MEDIA_ROOT SYSTEM_HEALTH_STATUS_FILE'''.split()
BOOL_SETTINGS = ['DEBUG', 'DEVELOPMENT', 'USE_ORACLEDB', 'IS_TEST_SERVER']
INT_SETTINGS = ['EMAIL_PORT', 'IDDS_MESSAGING_PORT']
LIST_SETTINGS = ['ADMIN_MAILS', 'FIRST_ADOPTERS', 'ALLOWED_HOSTS', 'CSRF_TRUSTED_ORIGINS']
DICT_SETTINGS = ['IAM', 'ESLogin', 'ATLAS_ES', 'ATLAS_OS', 'MONIT_ES', 'DKB_ES7',
                 'DKB_OS2', 'ANALYSIS_SETTING_CONF', 'VOMS_OPTIONS', 'defaultDatabase']
DEFAULTS = {
    'DEVELOPMENT': False, 'USE_ORACLEDB': False, 'IS_TEST_SERVER': False,
    'FIRST_ADOPTERS': [], 'defaultDatabase': {}, 'LOG_MODE': 'file',
    'AMI_API_V2_BASE_URL': 'https://atlas-ami.cern.ch/AMI2/api/',
    'AMI_API_V2_BASE_URL_REPLICA': 'https://ami.in2p3.fr/AMI2/api/',
    'CRIC_BASE_URL': 'https://atlas-cric.cern.ch/api',
    'JIRA_AUTH_URL': 'https://its.cern.ch/jira/loginCern.jsp',
    'JIRA_ISSUE_URL': 'https://its.cern.ch/jira/rest/api/2/issue/',
    'IDDS_MESSAGING_HOST': 'atlas-mb.cern.ch', 'IDDS_MESSAGING_PORT': 61013,
    'IDDS_MESSAGING_USERNAME': 'atlasidds',
    'IDDS_MESSAGING_QUEUE': '/queue/Consumer.prodsys.atlas.idds',
    'RUCIO_ACCOUNT': 'prodsys',
}
SEARCH_SETTINGS = ['ATLAS_ES', 'ATLAS_OS', 'MONIT_ES', 'DKB_ES7', 'DKB_OS2']
DB_FIELDS = ['CONN_HEALTH_CHECKS', 'CONN_MAX_AGE', 'DISABLE_SERVER_SIDE_CURSORS',
             'AUTOCOMMIT', 'ATOMIC_REQUESTS', 'ENGINE', 'VENDOR', 'NAME', 'USER',
             'PASSWORD', 'HOST', 'PORT', 'OPTIONS', 'TEST', 'TIME_ZONE']


def read_env_file(path):
    """Read literal KEY=value files; quotes/comments supported, no expansion/code."""
    values = {}
    for number, line in enumerate(Path(path).read_text().splitlines(), 1):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[7:]
        key, separator, raw = line.partition('=')
        key = key.strip()
        if not separator or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', key):
            raise ConfigurationError(f'{path}:{number}: expected KEY=value')
        if key in values:
            raise ConfigurationError(f'{path}:{number}: duplicate key {key}')
        try:
            parts = shlex.split(raw, comments=True, posix=True)
        except ValueError:
            raise ConfigurationError(f'{path}:{number}: invalid quoting for {key}') from None
        if len(parts) > 1:
            raise ConfigurationError(f'{path}:{number}: quote whitespace in {key}')
        values[key] = parts[0] if parts else ''
    return values


def boolean(value, key):
    if value.lower() not in ('true', 'false', '1', '0'):
        raise ConfigurationError(f'{key}: expected true/false or 1/0')
    return value.lower() in ('true', '1')


def json_value(value, key, expected):
    try:
        parsed = json.loads(value)
    except (ValueError, TypeError):
        raise ConfigurationError(f'{key}: expected JSON {expected.__name__}') from None
    if not isinstance(parsed, expected):
        raise ConfigurationError(f'{key}: expected JSON {expected.__name__}')
    return parsed


def load_environment(environ=None):
    """defaults < common.env < local.env < secrets.env < process environment.

    Files are required when ATLAS_CONFIG_DIR is set. Environment-only deployments
    set PRODSYS_CONFIG_MODE=environment and do not need files.
    """
    environ = dict(os.environ if environ is None else environ)
    values = {}
    directory = environ.get('ATLAS_CONFIG_DIR')
    if directory:
        for name in ('common.env', 'local.env', 'secrets.env'):
            path = Path(directory) / name
            if not path.is_file():
                raise ConfigurationError(f'Required configuration file missing: {path}')
            values.update(read_env_file(path))
    values.update(environ)
    config = copy.deepcopy(DEFAULTS)
    mappings = {name.upper(): name for name in STRING_SETTINGS + BOOL_SETTINGS + INT_SETTINGS + LIST_SETTINGS + DICT_SETTINGS}
    for key, value in values.items():
        if key.startswith('PRODSYS_') and key != 'PRODSYS_CONFIG_MODE':
            name = mappings.get(key[8:].split('__')[0])
            if name is None:
                raise ConfigurationError(f'Unknown configuration key: {key}')
            suffix = key[8:].split('__')[1:]
            if suffix:
                if name not in DICT_SETTINGS or len(suffix) != 1:
                    raise ConfigurationError(f'Invalid nested configuration key: {key}')
                field = suffix[0].lower()
                allowed = {
                    **{n: {'hosts', 'login', 'password', 'verify_certs', 'ca_cert'} for n in SEARCH_SETTINGS},
                    'IAM': {'client_id', 'secret', 'api_base'},
                    'ANALYSIS_SETTING_CONF': {'proxy_path', 'rucio_account', 'panda_account', 'base_script_path'},
                    'ESLogin': {'login', 'password'},
                    'VOMS_OPTIONS': {'vo', 'host', 'port', 'user_cert', 'user_key'},
                }
                if field not in allowed.get(name, set()):
                    raise ConfigurationError(f'Unknown nested configuration key: {key}')
                if field == 'hosts':
                    value = json_value(value, key, list)
                elif field == 'verify_certs':
                    value = boolean(value, key)
                elif field == 'port':
                    value = integer(value, key)
                config.setdefault(name, {})[field] = value
            elif name in BOOL_SETTINGS:
                config[name] = boolean(value, key)
            elif name in INT_SETTINGS:
                config[name] = integer(value, key)
            elif name in LIST_SETTINGS:
                config[name] = json_value(value, key, list)
            elif name in DICT_SETTINGS:
                raise ConfigurationError(f'{key}: use individual __FIELD variables')
            else:
                config[name] = value
        elif key.startswith('DB_CONN_'):
            field = next((f for f in DB_FIELDS if key.endswith('_' + f)), None)
            if not field:
                raise ConfigurationError(f'Unknown database configuration key: {key}')
            alias = key[8:-(len(field) + 1)].lower()
            if not alias:
                raise ConfigurationError(f'Invalid database configuration key: {key}')
            if field in ('OPTIONS', 'TEST'):
                value = json_value(value, key, dict)
            elif field in ('CONN_HEALTH_CHECKS', 'AUTOCOMMIT', 'ATOMIC_REQUESTS', 'DISABLE_SERVER_SIDE_CURSORS'):
                value = boolean(value, key)
            elif field == 'CONN_MAX_AGE':
                value = integer(value, key)
            config.setdefault('dbaccess', {}).setdefault(alias, {})[field] = value
    validate(config)
    return config


def integer(value, key):
    try:
        return int(value)
    except ValueError:
        raise ConfigurationError(f'{key}: expected integer') from None


def validate(config):
    def reject_placeholders(value, key):
        if isinstance(value, dict):
            for field, item in value.items():
                reject_placeholders(item, f'{key}.{field}')
        elif isinstance(value, list):
            for item in value:
                reject_placeholders(item, key)
        elif isinstance(value, str) and ('***' in value or value in ('CHANGE_ME', 'FIXME')):
            raise ConfigurationError(f'Placeholder configuration: {key}')

    for name, value in config.items():
        reject_placeholders(value, name)

    def required(value, key):
        if value is None or value == '' or value in ('***', 'CHANGE_ME', 'FIXME'):
            raise ConfigurationError(f'Missing or placeholder configuration: {key}')
    for name in ('MY_SECRET_KEY', 'MY_CELERY', 'PANDA_DB_SCHEMA', 'DEFT_DB_SCHEMA',
                 'CERTIFICATE_PATH', 'CERT_PEM_PATH', 'CERT_KEY_PEM_PATH', 'JEDI_BASE_URL',
                 'OIDC_SECRET', 'JIRA_KEY', 'BIGPANDA_TOKEN', 'IDDS_MESSAGING_PASSWORD'):
        required(config.get(name), name)
    if '***' in config['MY_CELERY'] or 'CHANGE_ME' in config['MY_CELERY']:
        raise ConfigurationError('Placeholder credentials: MY_CELERY')
    for name in LIST_SETTINGS:
        if name in config and (not all(isinstance(x, str) and x for x in config[name])):
            raise ConfigurationError(f'{name}: expected a list of nonempty strings')
    if not config.get('ADMIN_MAILS'):
        raise ConfigurationError('ADMIN_MAILS must contain at least one address')
    if config['LOG_MODE'] not in ('file', 'console'):
        raise ConfigurationError('LOG_MODE must be file or console')
    if config['LOG_MODE'] == 'file':
        required(config.get('LOG_ROOT'), 'LOG_ROOT')
    for name, fields in {
        **{n: ('hosts', 'login', 'password', 'verify_certs', 'ca_cert') for n in SEARCH_SETTINGS if n != 'DKB_ES7'},
        'IAM': ('client_id', 'secret', 'api_base'),
        'ANALYSIS_SETTING_CONF': ('proxy_path', 'rucio_account', 'panda_account', 'base_script_path'),
    }.items():
        for field in fields:
            required(config.get(name, {}).get(field), f'{name}.{field}')
        if name in SEARCH_SETTINGS and (not config[name]['hosts'] or not all(isinstance(x, str) and x for x in config[name]['hosts'])):
            raise ConfigurationError(f'{name}.hosts: expected nonempty hosts')
    databases = config.get('dbaccess', {})
    if 'default' not in databases:
        raise ConfigurationError('DB_CONN_DEFAULT_* is required')
    for alias, database in databases.items():
        vendor = database.pop('VENDOR', None)
        if vendor:
            engines = {'oracle': 'django.db.backends.oracle', 'postgresql': 'django.db.backends.postgresql', 'sqlite': 'django.db.backends.sqlite3'}
            if vendor not in engines:
                raise ConfigurationError(f'Unsupported database vendor: {alias}')
            database.setdefault('ENGINE', engines[vendor])
        for field in ('ENGINE', 'NAME'):
            required(database.get(field), f'dbaccess.{alias}.{field}')
        if database['ENGINE'] != 'django.db.backends.sqlite3':
            for field in ('USER', 'PASSWORD'):
                required(database.get(field), f'dbaccess.{alias}.{field}')
