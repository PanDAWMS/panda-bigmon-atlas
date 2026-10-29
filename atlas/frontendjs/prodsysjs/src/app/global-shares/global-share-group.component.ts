import {Component, computed, input, output} from '@angular/core';
import {MatButtonModule} from '@angular/material/button';
import {GlobalShareGroup} from './global-shares.models';

@Component({
  selector: 'app-global-share-group',
  imports: [MatButtonModule],
  templateUrl: './global-share-group.component.html',
  styleUrl: './global-share-group.component.css',
})
export class GlobalShareGroupComponent {
  group = input.required<GlobalShareGroup>();
  disabled = input(false);
  review = output<GlobalShareGroup>();
  resetGroup = output<GlobalShareGroup>();
  readonly invalidNames = computed(() => this.group().shares.filter(share => !share.valid).map(share => share.name).join(', '));
}
