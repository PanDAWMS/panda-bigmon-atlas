import {Component, computed, inject, Input, signal} from '@angular/core';
import {toSignal} from "@angular/core/rxjs-interop";
import {DatasetRecoveryService} from "../dataset-recovery.service";
import {BPTaskComponent} from "../../common/bptask/bptask.component";
import {JsonPipe, NgClass} from "@angular/common";
import {RucioURLPipe} from "../../derivation-exclusion/rucio-url.pipe";
import {FormsModule} from "@angular/forms";
import {MatButton} from "@angular/material/button";
import {MatFormField, MatLabel} from "@angular/material/form-field";
import {MatInput} from "@angular/material/input";
import {ActivatedRoute, Router, RouterLink} from "@angular/router";
import {MatProgressSpinner} from "@angular/material/progress-spinner";

@Component({
  selector: 'app-recovered-datasets',
  standalone: true,
  imports: [
    BPTaskComponent,
    NgClass,
    RucioURLPipe,
    JsonPipe,
    FormsModule,
    MatButton,
    MatFormField,
    MatInput,
    MatLabel,
    RouterLink,
    MatProgressSpinner
  ],
  templateUrl: './recovered-datasets.component.html',
  styleUrl: './recovered-datasets.component.css'
})
export class RecoveredDatasetsComponent {
  @Input() set filter(value: string|undefined) {
    if (value !== undefined) {
        this.filter$.set(value);
        this.formValue = value;
    }
  }
  filter$ = signal('');
  formValue = '';
  datasetRecoveryService = inject(DatasetRecoveryService);
  router = inject(Router);
  route = inject(ActivatedRoute);
  allRequests = toSignal(this.datasetRecoveryService.getAllRequests());
  goodRequests = computed(() =>
  this.allRequests()?.filter(request => (request.status === 'done' || request.status === 'running')).
    filter(request => (request.original_dataset.includes(this.filter$()) || request.requestor.includes(this.filter$()) ))
  );
  isLoading = this.datasetRecoveryService.state.isLoading;
  error = this.datasetRecoveryService.state.error;
  statusClass(status: string): string {
    switch (status) {
      case 'pending':
        return 'text-yellow-500';
      case 'submitted':
        return 'text-blue-500';
      case 'running':
        return 'text-green-500';
      case 'done':
        return 'text-green-800';
      default:
        return 'text-gray-500';
    }
  }

  changeRouterFilter($event: Event) {
    this.router.navigate(['./'], {queryParams: {filter: ($event.target as HTMLInputElement).value}, relativeTo: this.route});
  }
}
