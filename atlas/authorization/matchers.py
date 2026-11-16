"""Portable policy semantics; every constraint in a row must match."""

import json
import math


def match_role(roles, role):
    return role in roles


def match_object(obj, constraints):
    return all(key in obj and type(obj[key]) is type(value) and obj[key] == value
               for key, value in json.loads(constraints).items())


def validate_constraints(constraints):
    for value in constraints.values():
        if isinstance(value, list) and value and all(isinstance(x, str) for x in value):
            continue
        if isinstance(value, list) and len(value) == 2 and all(
                type(x) in (int, float) and math.isfinite(x) for x in value):
            if value[0] <= value[1]:
                continue
        # A documented extension for legacy upper bounds without inventing a floor.
        if isinstance(value, dict) and value and set(value) <= {'min', 'max'}:
            if all(type(x) in (int, float) and math.isfinite(x) for x in value.values()):
                if value.get('min', -math.inf) <= value.get('max', math.inf):
                    continue
        raise ValueError(f'Unsupported parameter constraint: {value!r}')


def match_constraints(params, constraints):
    for key, bounds in json.loads(constraints).items():
        if key not in params:
            return False
        value = params[key]
        if isinstance(bounds, list) and all(isinstance(x, str) for x in bounds):
            if value not in bounds:
                return False
        else:
            if isinstance(value, bool):
                return False
            try:
                value = int(value)
            except (TypeError, ValueError, OverflowError):
                return False
            lower, upper = ((bounds.get('min', -math.inf), bounds.get('max', math.inf))
                            if isinstance(bounds, dict) else bounds)
            if not lower <= value <= upper:
                return False
    return True
