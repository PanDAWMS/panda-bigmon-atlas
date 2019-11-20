import os

from django.core.wsgi import get_wsgi_application
import sys
from os.path import join, pardir,  dirname

PYTHONPATH = [
    join(dirname(__file__), pardir),
]

# inject few paths to pythonpath
for p in PYTHONPATH:
    if p not in sys.path:
        sys.path.insert(0, p)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "atlas.settings")

application = get_wsgi_application()