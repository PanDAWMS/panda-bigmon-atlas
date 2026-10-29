import {Component, computed, input, output, signal} from '@angular/core';
import {DecimalPipe} from '@angular/common';
import {FormsModule} from '@angular/forms';
import {GlobalShareGroupComponent} from './global-share-group.component';
import {GlobalShareGroup, globalShareTreeRows} from './global-shares.models';

@Component({
  selector: 'app-global-share-tree',
  imports: [DecimalPipe, FormsModule, GlobalShareGroupComponent],
  templateUrl: './global-share-tree.component.html',
  styleUrl: './global-share-tree.component.css',
})
export class GlobalShareTreeComponent {
  groups = input.required<GlobalShareGroup[]>();
  disabled = input(false);
  errorMessage = input('');
  statusMessage = input('');
  valueChange = output<{name: string; value: number | null}>();
  review = output<GlobalShareGroup>();
  resetGroup = output<GlobalShareGroup>();

  readonly rows = computed(() => globalShareTreeRows(this.groups()));
  readonly levels = computed(() => Array.from({length: Math.max(1, ...this.rows().map(row => row.depth + 1))}, (_, i) => i));
  readonly selectedParent = signal<string | null | undefined>(undefined);
  readonly activeGroup = computed(() => this.groups().find(group => group.parent === this.selectedParent()) ??
    this.groups().find(group => group.shares.some(share => share.editable)) ?? this.groups()[0]);

  selectGroup(parent: string | null): void {
    this.selectedParent.set(parent);
  }

  errorId(name: string): string {
    return 'share-error-' + encodeURIComponent(name);
  }
}
