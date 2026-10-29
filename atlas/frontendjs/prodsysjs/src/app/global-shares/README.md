# Global shares

New Angular page: `/ng/global-shares/`.

The page loads `GET /gdpconfig/global_share_tree_v2/`:

```json
{
  "shares": [
    {"name": "Derivations", "parent": null, "value": 30},
    {"name": "DAOD", "parent": "Derivations", "value": 100}
  ],
  "manageable_shares": ["DAOD"]
}
```

The flat parent links represent the entire tree, including read-only shares.
The page renders a depth-first tree, placing each child below its parent in the
next level column, matching the legacy layout. All branches stay expanded.
Only names in `manageable_shares` have highlighted nodes and editable inputs.
The permission list mirrors the existing `global_share_change` rules: direct
GDPConfig permission grants all shares; DPD and REPR grant direct children of
Derivations and Reprocessing; MCPROD grants the four MC level-two shares and
their direct children.

Global percentages use the sibling total to normalize each allocation and
propagate it through the parent hierarchy, matching the legacy preview.
Each sibling group must total 100%, with integer values from 0 to 100, to save.
Inputs fit three digits and sit beside the share name and global percentage.
A sticky bar shows the selected group's total, validation errors, and review
and reset buttons. Focusing an input selects its group; the group selector can
also select any group (asterisks mark modified groups).
Review and save one group at a time: the legacy writer authorizes batches by
scope, so a batch combining different MC levels or production groups can fail.

Saves use the existing `POST /gdpconfig/global_share_change/` without changing
that endpoint. The page reloads server values after an accepted request and
keeps edits in other groups. It also retains pending edits if the returned
values do not reflect the requested allocation.

The legacy page, templates, and API implementations are unchanged. Backend
tests in `atlas/gdpconfig/test_global_share_api.py` cover only the new read API
and its permission list; they never call the share change endpoint.
