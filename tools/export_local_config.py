#!/usr/bin/env python3
"""Convert literal local.py assignments to common/local/secrets env templates.

Never imports or executes the source. Review classification before deployment.
"""
import argparse
import ast
import json
import os
from pathlib import Path
import shlex

LOCAL = {'LOG_ROOT', 'DEVELOPMENT', 'USE_ORACLEDB', 'IS_TEST_SERVER',
         'FAKE_LOGIN_AS_USER', 'CERTIFICATE_PATH', 'CERT_PEM_PATH', 'CERT_KEY_PEM_PATH',
         'VOMS_PROXY_CERT', 'EMAIL_HOST', 'EMAIL_PORT', 'ORACLE_CONFIG_DIR', 'LOG_MODE'}
SECRETS = {'MY_SECRET_KEY', 'IDDS_MESSAGING_PASSWORD', 'JIRA_KEY', 'MY_CELERY',
           'DEFT_AUTH_USER', 'DEFT_AUTH_KEY', 'OPENAI_KEY', 'OIDC_SECRET',
           'BIGPANDA_TOKEN', 'CELERY_CONNECTION'}


def assignments(path):
    result = {}
    for node in ast.parse(Path(path).read_text()).body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            raise ValueError('Only literal name assignments are supported; source was not executed')
        try:
            result[node.targets[0].id] = ast.literal_eval(node.value)
        except (ValueError, TypeError):
            raise ValueError(f'Non-literal assignment: {node.targets[0].id}') from None
    return result


def convert(config):
    files = {name: [] for name in ('common', 'local', 'secrets')}
    def emit(group, key, value):
        if isinstance(value, (list, dict, bool, int)):
            value = json.dumps(value, separators=(',', ':'))
        files[group].append(f'{key}={shlex.quote(str(value))}')
    for name, value in config.items():
        if name == 'dbaccess':
            for alias, database in value.items():
                for field, item in database.items():
                    emit('secrets' if field in ('USER', 'PASSWORD') else 'common',
                         f'DB_CONN_{alias.upper()}_{field}', item)
        elif isinstance(value, dict):
            for field, item in value.items():
                group = 'secrets' if field in ('login', 'password', 'secret') else 'common'
                if field in ('ca_cert', 'proxy_path', 'base_script_path', 'user_cert', 'user_key'):
                    group = 'local'
                emit(group, f'PRODSYS_{name.upper()}__{field.upper()}', item)
        else:
            emit('secrets' if name in SECRETS else 'local' if name in LOCAL else 'common',
                 f'PRODSYS_{name.upper()}', value)
    return {name: '\n'.join(lines) + '\n' for name, lines in files.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source')
    parser.add_argument('destination')
    args = parser.parse_args()
    target = Path(args.destination)
    target.mkdir(parents=True, exist_ok=True)
    for name, content in convert(assignments(args.source)).items():
        path = target / f'{name}.env'
        # Refuse accidental overwrites and protect credentials from creation.
        with open(path, 'x', opener=lambda p, flags: os.open(p, flags, 0o600)) as output:
            output.write(content)


if __name__ == '__main__':
    main()
