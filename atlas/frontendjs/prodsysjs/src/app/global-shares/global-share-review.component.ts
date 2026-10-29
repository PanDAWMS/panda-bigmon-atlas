import {Component, inject} from '@angular/core';
import {MAT_DIALOG_DATA, MatDialogModule} from '@angular/material/dialog';
import {MatButtonModule} from '@angular/material/button';
import {GlobalShareGroup} from './global-shares.models';

@Component({
  selector: 'app-global-share-review',
  imports: [MatDialogModule, MatButtonModule],
  template: `
    <h2 mat-dialog-title>Review global share changes</h2>
    <mat-dialog-content>
      <p>{{ group.path.join(' / ') }}</p>
      <table>
        <thead><tr><th scope="col">Share</th><th scope="col">Current</th><th scope="col">New</th></tr></thead>
        <tbody>
          @for (share of group.shares; track share.name) {
            @if (share.changed) {
              <tr><th scope="row">{{ share.name }}</th><td>{{ share.value }}%</td><td>{{ share.draft }}%</td></tr>
            }
          }
        </tbody>
      </table>
      <p>Group total: {{ group.total }}%</p>
    </mat-dialog-content>
    <mat-dialog-actions align="end">
      <button mat-button [mat-dialog-close]="false">Cancel</button>
      <button mat-raised-button color="primary" [mat-dialog-close]="true">Save group</button>
    </mat-dialog-actions>
  `,
  styles: `
    table { width: 100%; text-align: left; border-collapse: collapse; }
    th, td { padding: .6rem; border-bottom: 1px solid #ddd; }
  `,
})
export class GlobalShareReviewComponent {
  readonly group = inject<GlobalShareGroup>(MAT_DIALOG_DATA);
}
