"""Django settings assembled from resolved deployment configuration.

runtime.py selects legacy local.py or common/local/secrets environment files
before base.py and config.py derive Django settings. See deploy/README.md.
"""

from atlas.settings.base import *

from atlas.settings.config import *

# Preserve late overrides only for legacy deployments. Environment mode must
# never import a stale local.py or let it replace the resolved configuration.
from .runtime import ENVIRONMENT_CONFIG
if ENVIRONMENT_CONFIG:
    from .runtime import *
else:
    from .local import *
