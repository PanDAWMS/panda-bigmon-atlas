"""Resolve configuration before any Django settings are derived."""
import importlib
import os
from atlas.configuration import ConfigurationError, DEFAULTS, load_environment

_mode = os.environ.get('PRODSYS_CONFIG_MODE', 'environment' if os.environ.get('ATLAS_CONFIG_DIR') else 'legacy')
if _mode not in ('environment', 'legacy'):
    raise ConfigurationError('PRODSYS_CONFIG_MODE must be environment or legacy')
if _mode == 'legacy' and os.environ.get('ATLAS_CONFIG_DIR'):
    raise ConfigurationError('ATLAS_CONFIG_DIR cannot be used in legacy mode')
ENVIRONMENT_CONFIG = _mode == 'environment'
_configuration_method = (
    'environment files (common.env -> local.env -> secrets.env -> process environment)'
    if os.environ.get('ATLAS_CONFIG_DIR') else
    'process environment only' if ENVIRONMENT_CONFIG else
    'legacy atlas.settings.local'
)
if ENVIRONMENT_CONFIG:
    _config = load_environment()
else:
    _legacy = importlib.import_module('atlas.settings.local')
    _config = {**DEFAULTS, **{k: v for k, v in vars(_legacy).items() if not k.startswith('_')}}
globals().update(_config)
__all__ = list(_config)


def get_setting(name, default=None):
    return _config.get(name, default)


def get_configuration_method():
    """Describe the selected source without exposing configuration values."""
    return _configuration_method
