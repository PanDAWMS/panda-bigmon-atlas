export interface GlobalShare {
  name: string;
  parent: string | null;
  value: number;
}

export interface GlobalShareTree {
  shares: GlobalShare[];
  manageable_shares: string[];
}

export interface GlobalShareRow extends GlobalShare {
  draft: number | null;
  editable: boolean;
  changed: boolean;
  valid: boolean;
  percentage: number | null;
}

export interface GlobalShareGroup {
  parent: string | null;
  path: string[];
  shares: GlobalShareRow[];
  total: number | null;
  changed: boolean;
  valid: boolean;
}

export type ShareDrafts = Record<string, number | null>;

export interface GlobalShareTreeRow {
  share: GlobalShareRow;
  depth: number;
  children: GlobalShareGroup | undefined;
}

export function validShareValue(value: number | null): boolean {
  return value !== null && Number.isInteger(value) && value >= 0 && value <= 100;
}

/** Build sibling groups and propagate normalized percentages from the root. */
export function globalShareGroups(data: GlobalShareTree, drafts: ShareDrafts): GlobalShareGroup[] {
  const editable = new Set(data.manageable_shares);
  const children = new Map<string | null, GlobalShare[]>();
  for (const share of data.shares) {
    const siblings = children.get(share.parent) ?? [];
    siblings.push(share);
    children.set(share.parent, siblings);
  }
  const groups: GlobalShareGroup[] = [];
  const visited = new Set<string | null>();

  const visit = (parent: string | null, path: string[], percentage: number | null): void => {
    if (visited.has(parent) || !children.has(parent)) {
      return;
    }
    visited.add(parent);
    const rows = children.get(parent).map(share => {
      const canEdit = editable.has(share.name);
      const draft = canEdit && Object.hasOwn(drafts, share.name) ? drafts[share.name] : share.value;
      return {
        ...share, draft, editable: canEdit, changed: draft !== share.value,
        valid: validShareValue(draft), percentage: null,
      } as GlobalShareRow;
    });
    const total = rows.every(share => share.valid) ? rows.reduce((sum, share) => sum + share.draft, 0) : null;
    for (const row of rows) {
      row.percentage = total === null || percentage === null ? null : total === 0 ? 0 : row.draft / total * percentage;
    }
    groups.push({parent, path, shares: rows, total, changed: rows.some(share => share.changed), valid: total === 100});
    for (const row of rows) {
      visit(row.name, [...path, row.name], row.percentage);
    }
  };

  visit(null, ['All shares'], 100);
  // Keep every share visible even if the data contains a missing parent or a cycle.
  for (const parent of children.keys()) {
    visit(parent, [parent ?? 'All shares'], null);
  }
  return groups;
}

/** Place every parent immediately before its descendants, as in the legacy tree. */
export function globalShareTreeRows(groups: GlobalShareGroup[]): GlobalShareTreeRow[] {
  const children = new Map(groups.map(group => [group.parent, group]));
  const rows: GlobalShareTreeRow[] = [];
  const visited = new Set<string>();
  const visit = (group: GlobalShareGroup | undefined, depth: number): void => {
    for (const share of group?.shares ?? []) {
      if (visited.has(share.name)) {
        continue;
      }
      visited.add(share.name);
      const descendants = children.get(share.name);
      rows.push({share, depth, children: descendants});
      visit(descendants, depth + 1);
    }
  };
  visit(children.get(null), 0);
  // Preserve visibility of detached branches and cycles, too.
  for (const group of groups) {
    visit(group, 0);
  }
  return rows;
}
