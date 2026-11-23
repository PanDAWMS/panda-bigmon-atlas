"""Django-independent Casbin engine and JSON-aware policy loader."""

import hashlib
import json
from pathlib import Path

from .matchers import match_constraints, match_object, match_role, validate_constraints

ROOT = Path(__file__).parent
DEFAULT_POLICIES = (
    ROOT / 'policies/shared/production_tasks.csv',
    ROOT / 'policies/shared/analysis_tasks.csv',
    ROOT / 'policies/prodsys/task_superusers.csv',
    ROOT / 'policies/prodsys/replication_rules.csv',
    ROOT / 'policies/prodsys/delegation.csv',
    ROOT / 'policies/prodsys/requests.csv',
    ROOT / 'policies/prodsys/global_shares.csv',
    ROOT / 'policies/prodsys/configuration.csv',
    ROOT / 'policies/shared/analysis_scopes.csv',
    ROOT / 'policies/prodsys/administration.csv',
)


def parse_policy(line):
    """Read the documented inline-JSON notation, including embedded commas."""
    prefix = line.split(',', 4)
    if len(prefix) != 5 or prefix[0].strip() != 'p':
        raise ValueError('Expected p, role, obj_type, act, object JSON, parameter JSON, allow')
    fields = [field.strip() for field in prefix[1:4]]
    rest = prefix[4].lstrip()
    decoder = json.JSONDecoder()
    for parameter_constraints in (False, True):
        value, end = decoder.raw_decode(rest)
        if not isinstance(value, dict):
            raise ValueError('Constraints must be JSON objects')
        if parameter_constraints:
            validate_constraints(value)
        fields.append(json.dumps(value, sort_keys=True, separators=(',', ':')))
        rest = rest[end:].lstrip()
        if not rest.startswith(','):
            raise ValueError('Missing constraint separator')
        rest = rest[1:].lstrip()
    if rest != 'allow' or not all(fields[:3]):
        raise ValueError('Only explicit allow policies with nonempty names are supported')
    return fields + ['allow']


class AuthorizationEngine:
    def __init__(self, policy_paths=None):
        import casbin

        model = (ROOT / 'model.conf').read_text()
        digest = hashlib.sha256(model.encode())
        self.enforcer = casbin.Enforcer(str(ROOT / 'model.conf'))
        for name, function in (('match_role', match_role), ('match_object', match_object),
                               ('match_constraints', match_constraints)):
            self.enforcer.add_function(name, function)
        self.references = {}
        self.operations = set()
        for path in DEFAULT_POLICIES if policy_paths is None else policy_paths:
            path = Path(path)
            data = path.read_bytes()
            digest.update(path.name.encode() + b'\0' + data)
            for number, line in enumerate(data.decode().splitlines(), 1):
                if not line.strip() or line.lstrip().startswith('#'):
                    continue
                try:
                    row = parse_policy(line)
                except (ValueError, TypeError) as error:
                    raise ValueError(f'{path}:{number}: {error}') from error
                key = tuple(row)
                self.references.setdefault(key, []).append(f'{path.parent.name}/{path.name}:{number}')
                self.operations.add((row[1], row[2]))
                self.enforcer.add_policy(*row)
        self.bundle_hash = digest.hexdigest()

    def evaluate(self, roles, obj_type, act, obj, params):
        allowed, rule = self.enforcer.enforce_ex(roles, obj_type, act, obj, params)
        return bool(allowed), self.references.get(tuple(rule), [])

    def enforce(self, roles, obj_type, act, obj, params):
        return self.evaluate(roles, obj_type, act, obj, params)[0]


if __name__ == '__main__':
    engine = AuthorizationEngine()
    print(f'Validated {len(engine.references)} policies; bundle={engine.bundle_hash}')
