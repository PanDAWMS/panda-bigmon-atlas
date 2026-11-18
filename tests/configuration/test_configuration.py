"""Run without Django, Oracle, databases or network:
python3 -m unittest discover -s tests/configuration -v
"""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from atlas.configuration import ConfigurationError, load_environment, read_env_file
from tools.export_local_config import assignments
from tools.render_config import manifest

ROOT = Path(__file__).resolve().parents[2]


def example_values():
    values = {}
    for name in ('common', 'local', 'secrets'):
        values.update(read_env_file(ROOT / f'deploy/config/{name}.env.example'))
    values = {k: v.replace('***', 'test-credential') for k, v in values.items()}
    values['PRODSYS_ANALYSIS_SETTING_CONF__RUCIO_ACCOUNT'] = 'test-account'
    values['PRODSYS_ADMIN_MAILS'] = '["ops@example.org"]'
    return values


class ConfigurationTests(unittest.TestCase):
    def test_all_aliases_and_nested_shapes(self):
        config = load_environment(example_values())
        self.assertEqual(len(config['dbaccess']), 14)
        self.assertEqual(config['dbaccess']['dev_db_wr']['USER'], 'atlas_deft_w')
        self.assertEqual(config['dbaccess']['deft']['OPTIONS'], {})
        self.assertEqual(config['ATLAS_OS']['verify_certs'], True)
        self.assertEqual(config['ANALYSIS_SETTING_CONF']['rucio_account'], 'test-account')

    def test_file_precedence_and_process_override_in_production(self):
        with tempfile.TemporaryDirectory() as directory:
            for name in ('common', 'local', 'secrets'):
                (Path(directory) / f'{name}.env').write_text(
                    (ROOT / f'deploy/config/{name}.env.example').read_text().replace('***', 'test-credential'))
            with (Path(directory) / 'secrets.env').open('a') as output:
                output.write('PRODSYS_DEVELOPMENT=false\n')
            config = load_environment({'ATLAS_CONFIG_DIR': directory})
            self.assertFalse(config['DEVELOPMENT'])
            config = load_environment({'ATLAS_CONFIG_DIR': directory, 'PRODSYS_DEVELOPMENT': 'true'})
            self.assertTrue(config['DEVELOPMENT'])

    def test_openshift_environment_only(self):
        values = example_values()
        values.update(read_env_file(ROOT / 'deploy/config/openshift-local.env.example'))
        config = load_environment(values)
        self.assertFalse(config['DEVELOPMENT'])
        self.assertEqual(config['LOG_MODE'], 'console')

    def test_missing_files_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ConfigurationError):
                load_environment({'ATLAS_CONFIG_DIR': directory, **example_values()})

    def test_placeholders_fail_without_values_in_error(self):
        values = example_values()
        values['DB_CONN_DEFT_PASSWORD'] = '***'
        with self.assertRaises(ConfigurationError) as context:
            load_environment(values)
        self.assertIn('deft.PASSWORD', str(context.exception))
        self.assertNotIn('***', str(context.exception))

    def test_invalid_types_and_unknown_keys(self):
        for key, value in [('PRODSYS_DEVELOPMENT', 'maybe'), ('PRODSYS_ADMIN_MAILS', 'admin@example.org'),
                           ('PRODSYS_ADMIN_MAILS', '[1]'), ('PRODSYS_EMAIL_PORT', 'smtp'),
                           ('DB_CONN_DEFAULT_OPTIONS', '[1]'), ('PRODSYS_TYPO', 'x'),
                           ('PRODSYS_ATLAS_OS__TYPO', 'x'), ('PRODSYS_ATLAS_OS__VERIFY_CERTS', 'maybe')]:
            with self.subTest(key=key), self.assertRaises(ConfigurationError):
                load_environment({**example_values(), key: value})

    def test_database_alias_underscores_and_options(self):
        config = load_environment({**example_values(), 'DB_CONN_DEV_DB_WR_CONN_MAX_AGE': '60',
                                   'DB_CONN_DEV_DB_WR_OPTIONS': '{"threaded":true}'})
        self.assertEqual(config['dbaccess']['dev_db_wr']['CONN_MAX_AGE'], 60)
        self.assertEqual(config['dbaccess']['dev_db_wr']['OPTIONS'], {'threaded': True})

    def test_parser_is_literal_and_rejects_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'test.env'
            path.write_text('KEY=\'$(touch /tmp/should-not-exist) ${HOME} # literal\'\nEMPTY=\n')
            self.assertEqual(read_env_file(path)['KEY'], '$(touch /tmp/should-not-exist) ${HOME} # literal')
            self.assertEqual(read_env_file(path)['EMPTY'], '')
            path.write_text('KEY=one\nKEY=two\n')
            with self.assertRaises(ConfigurationError):
                read_env_file(path)

    def test_exporter_does_not_execute_python(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'local.py'
            path.write_text('import os\nos.system("false")\n')
            with self.assertRaises(ValueError):
                assignments(path)

    def test_kubernetes_values_preserve_quotes_json_and_credentials(self):
        values = {}
        for name in ('common', 'local', 'secrets'):
            resource = manifest(ROOT / f'deploy/config/{name}.env.example',
                                'prodsys-' + name, secret=name == 'secrets')
            self.assertEqual(resource['kind'], 'Secret' if name == 'secrets' else 'ConfigMap')
            values.update(resource['stringData' if name == 'secrets' else 'data'])
        values = {k: v.replace('***', 'test-credential') for k, v in values.items()}
        values['PRODSYS_ADMIN_MAILS'] = '["ops@example.org"]'
        config = load_environment(values)
        self.assertEqual(config['ADMIN_MAILS'], ['ops@example.org'])
        self.assertEqual(config['MY_SECRET_KEY'], 'test-credential')

    def test_optional_credentials_are_not_allowed_to_remain_placeholders(self):
        with self.assertRaises(ConfigurationError):
            load_environment({**example_values(), 'PRODSYS_OPENAI_KEY': '***'})

    def test_environment_settings_and_console_logging_without_local_py(self):
        code = '''
import logging.config
import sys
import atlas.settings as settings
assert 'atlas.settings.local' not in sys.modules
assert settings.DEBUG is False
assert settings.DATABASES['deft']['NAME'] == 'ADCR_PANDA'
assert settings.CELERY_BROKER_URL == settings.MY_CELERY
from atlas.settings.logconfig import LOGGING
assert all('filename' not in h for h in LOGGING['handlers'].values())
# JSON formatter is an optional runtime dependency; exercise standard handlers.
LOGGING['handlers'].pop('json_console')
LOGGING['formatters'].pop('json')
LOGGING['filters'].pop('require_debug_false')
LOGGING['handlers']['mail_admins']['filters'] = []
for logger in LOGGING['loggers'].values():
    logger['handlers'] = ['console']
from atlas.settings.logconfig import configure_logging
configure_logging(LOGGING)
from atlas.settings import amiclient, messaging, jiraclient
assert amiclient.AMI_API_V2_BASE_URL == 'https://ami.example.org/'
assert messaging.IDDS_PRODUCTION_CONFIG['connection']['host'] == 'mq.example.org'
assert jiraclient.JIRA_CONFIG['auth_url'] == 'https://jira.example.org/'
'''
        env = {**os.environ, **example_values(), 'PRODSYS_CONFIG_MODE': 'environment',
               'PRODSYS_USE_ORACLEDB': 'false', 'PRODSYS_DEVELOPMENT': 'false',
               'PRODSYS_LOG_MODE': 'console', 'PRODSYS_AMI_API_V2_BASE_URL': 'https://ami.example.org/',
               'PRODSYS_IDDS_MESSAGING_HOST': 'mq.example.org',
               'PRODSYS_JIRA_AUTH_URL': 'https://jira.example.org/'}
        env.pop('ATLAS_CONFIG_DIR', None)
        result = subprocess.run([sys.executable, '-c', code], cwd=ROOT, env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Configuration loading method: process environment only', result.stderr)

    def test_legacy_settings_keep_late_overrides(self):
        code = '''
import sys, types
legacy = types.ModuleType('atlas.settings.local')
legacy.__dict__.update(CONFIG)
legacy.DEBUG = False
legacy.CELERY_BROKER_URL = 'legacy-override'
sys.modules['atlas.settings.local'] = legacy
import atlas.settings as settings
assert settings.CELERY_BROKER_URL == 'legacy-override'
assert settings.DEBUG is False
assert settings.BIGPANDA_TOKEN == CONFIG['BIGPANDA_TOKEN']
'''
        config = load_environment(example_values())
        config['USE_ORACLEDB'] = False
        code = 'CONFIG = ' + repr(config) + '\n' + code
        env = {k: v for k, v in os.environ.items() if k not in ('ATLAS_CONFIG_DIR', 'PRODSYS_CONFIG_MODE')}
        result = subprocess.run([sys.executable, '-c', code], cwd=ROOT, env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
