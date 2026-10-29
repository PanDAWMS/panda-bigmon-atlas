import {Component, computed, DestroyRef, inject, signal} from '@angular/core';
import {takeUntilDestroyed} from '@angular/core/rxjs-interop';
import {MatButtonModule} from '@angular/material/button';
import {MatDialog} from '@angular/material/dialog';
import {MatProgressBarModule} from '@angular/material/progress-bar';
import {switchMap} from 'rxjs/operators';
import {GlobalShareTreeComponent} from './global-share-tree.component';
import {GlobalShareReviewComponent} from './global-share-review.component';
import {GlobalShareGroup, globalShareGroups, GlobalShareTree, ShareDrafts} from './global-shares.models';
import {globalShareError, GlobalSharesService} from './global-shares.service';

@Component({
  selector: 'app-global-shares',
  imports: [GlobalShareTreeComponent, MatButtonModule, MatProgressBarModule],
  templateUrl: './global-shares.component.html',
  styleUrl: './global-shares.component.css',
})
export class GlobalSharesComponent {
  private readonly service = inject(GlobalSharesService);
  private readonly dialog = inject(MatDialog);
  private readonly destroyRef = inject(DestroyRef);

  readonly data = signal<GlobalShareTree>({shares: [], manageable_shares: []});
  readonly drafts = signal<ShareDrafts>({});
  readonly loading = signal(false);
  readonly saving = signal(false);
  readonly error = signal('');
  readonly message = signal('');
  readonly groups = computed(() => globalShareGroups(this.data(), this.drafts()));
  readonly changedCount = computed(() => this.groups().reduce(
    (count, group) => count + group.shares.filter(share => share.changed).length, 0));
  readonly logsUrl = 'https://os-atlas.cern.ch/dashboards/app/data-explorer/discover/#?_a=(discover:(columns:!(user,message,container,number_of_extension,_source),interval:auto,sort:!()),metadata:(indexPattern:bce7ecb0-7533-11eb-ba28-77fe4323ac05,view:discover))&_q=(filters:!((\'$state\':(store:appState),meta:(alias:!n,disabled:!f,index:bce7ecb0-7533-11eb-ba28-77fe4323ac05,key:funcName,negate:!f,params:(query:global_share_change),type:phrase),query:(match_phrase:(funcName:global_share_change)))),query:(language:kuery,query:\'\'))&_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-1y,to:now))';

  constructor() {
    this.load();
  }

  load(): void {
    if (this.loading() || this.saving()) {
      return;
    }
    this.loading.set(true);
    this.error.set('');
    this.message.set('');
    this.service.getTree().pipe(takeUntilDestroyed(this.destroyRef)).subscribe({
      next: data => {
        this.acceptData(data);
        this.loading.set(false);
      },
      error: error => {
        this.error.set(globalShareError(error));
        this.loading.set(false);
      },
    });
  }

  changeValue({name, value}: {name: string; value: number | null}): void {
    if (this.loading() || this.saving() || !this.data().manageable_shares.includes(name)) {
      return;
    }
    this.drafts.update(drafts => ({...drafts, [name]: value}));
    this.error.set('');
    this.message.set('');
  }

  resetGroup(group: GlobalShareGroup): void {
    const names = new Set(group.shares.map(share => share.name));
    this.drafts.update(drafts => Object.fromEntries(Object.entries(drafts).filter(([name]) => !names.has(name))));
    this.error.set('');
    this.message.set('');
  }

  review(group: GlobalShareGroup): void {
    if (!group.valid || !group.changed || this.loading() || this.saving()) {
      return;
    }
    this.dialog.open(GlobalShareReviewComponent, {data: group, width: '600px', maxWidth: '95vw'})
      .afterClosed().pipe(takeUntilDestroyed(this.destroyRef)).subscribe(confirmed => {
        if (confirmed === true) {
          this.saveGroup(group.parent);
        }
      });
  }

  private saveGroup(parent: string | null): void {
    const group = this.groups().find(candidate => candidate.parent === parent);
    if (!group?.valid || !group.changed || this.loading() || this.saving()) {
      return;
    }
    const changes = Object.fromEntries(group.shares.filter(share => share.editable && share.changed)
      .map(share => [share.name, share.draft as number]));
    if (Object.keys(changes).length === 0) {
      return;
    }
    this.saving.set(true);
    this.error.set('');
    this.message.set('');
    let accepted = false;
    this.service.changeShares(changes).pipe(
      switchMap(result => {
        if (result.result !== 'OK') {
          throw new Error(result.exception || 'The share change request failed.');
        }
        accepted = true;
        return this.service.getTree();
      }),
      takeUntilDestroyed(this.destroyRef),
    ).subscribe({
      next: data => {
        this.acceptData(data);
        this.saving.set(false);
        const matches = Object.entries(changes).every(([name, value]) =>
          data.shares.some(share => share.name === name && share.value === value));
        if (matches) {
          this.message.set('Changes saved for ' + group.path.join(' / ') + '.');
        } else {
          this.error.set('The API accepted the request, but refreshed values differ from the requested values. Your pending edits have been kept.');
        }
      },
      error: error => {
        this.saving.set(false);
        this.error.set((accepted ? 'The request was accepted, but values could not be refreshed. Refresh before retrying. ' : '') + globalShareError(error));
      },
    });
  }

  private acceptData(data: GlobalShareTree): void {
    // Preserve pending edits in other groups and remove drafts confirmed by the server.
    const editable = new Set(data.manageable_shares);
    const currentValues = new Map(data.shares.map(share => [share.name, share.value]));
    this.drafts.update(drafts => Object.fromEntries(Object.entries(drafts).filter(([name, value]) =>
      editable.has(name) && currentValues.has(name) && currentValues.get(name) !== value)));
    this.data.set(data);
  }
}
