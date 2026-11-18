#!/usr/bin/env python3
"""Render one literal .env file as a ConfigMap or Secret JSON manifest.

Parse quotes before rendering: kubectl/oc --from-env-file keeps quote characters.
Secret manifests go to stdout; pipe directly to oc rather than committing them.
"""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from atlas.configuration import read_env_file


def manifest(path, name, secret=False):
    return {
        'apiVersion': 'v1',
        'kind': 'Secret' if secret else 'ConfigMap',
        'metadata': {'name': name},
        **({'type': 'Opaque'} if secret else {}),
        'stringData' if secret else 'data': read_env_file(path),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('path')
    parser.add_argument('name')
    parser.add_argument('--secret', action='store_true')
    args = parser.parse_args()
    print(json.dumps(manifest(args.path, args.name, args.secret), indent=2))


if __name__ == '__main__':
    main()
