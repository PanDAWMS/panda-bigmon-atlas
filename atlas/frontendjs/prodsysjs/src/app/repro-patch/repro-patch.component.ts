import {Component, computed, inject, Input} from '@angular/core';
import {ReproPatchService} from "./repro-patch.service";
import {AsyncPipe, DecimalPipe, JsonPipe} from "@angular/common";
import {MatProgressSpinner} from "@angular/material/progress-spinner";
import {MatFormField, MatLabel} from "@angular/material/form-field";
import {MatInput} from "@angular/material/input";
import {FormsModule, ReactiveFormsModule} from "@angular/forms";
import {MatButton} from "@angular/material/button";

@Component({
  selector: 'app-repro-patch',
  standalone: true,
  imports: [
    JsonPipe,
    AsyncPipe,
    MatProgressSpinner,
    MatFormField,
    MatInput,
    MatLabel,
    ReactiveFormsModule,
    FormsModule,
    DecimalPipe,
    MatButton
  ],
  templateUrl: './repro-patch.component.html',
  styleUrl: './repro-patch.component.css'
})
export class ReproPatchComponent {

  @Input() set requestID(value: string) {
    this.requestPageID = value;
    this.repoPatchService.setRequestID(value);
  }
  repoPatchService = inject(ReproPatchService);
  amiTag = '';
  requestPageID: string;

  isLoading = this.repoPatchService.state.isLoading;
  error = this.repoPatchService.state.error;
  taskPatchData = this.repoPatchService.state.taskPatchData;
  patched = this.repoPatchService.state.patched;
  tasksToAbort = this.repoPatchService.state.tasksToAbort;
  containers = this.repoPatchService.state.containers;

  submitPatch(): void {
    this.repoPatchService.applyPatch(this.amiTag);
  }
}
