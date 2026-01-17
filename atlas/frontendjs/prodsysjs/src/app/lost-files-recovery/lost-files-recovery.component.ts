import {Component, computed, effect, inject, input, OnChanges, signal} from '@angular/core';
import {LostFileRecoveryService} from "./lost-file-recovery.service";
import {DatePipe, JsonPipe} from "@angular/common";
import {MatProgressSpinner} from "@angular/material/progress-spinner";
import {HttpErrorResponse} from "@angular/common/http";
import {setErrorMessage} from "../dsid-info/dsid-info.service";
import {RucioDIDComponent} from "../production-request/rucio-did/rucio-did.component";
import {DatasetSizePipe} from "../derivation-exclusion/dataset-size.pipe";
import {RucioURLPipe} from "../derivation-exclusion/rucio-url.pipe";
import {MatButtonToggle} from "@angular/material/button-toggle";
import {MatSlideToggle} from "@angular/material/slide-toggle";
import {MatButton} from "@angular/material/button";
import {AsyncTaskProgressComponent} from "../common/async-task-progress/async-task-progress.component";
import {MatCard, MatCardContent, MatCardTitle} from "@angular/material/card";
import {AsyncProdTaskSplitStatus} from "../production-request/production-request.service";
import {TaskService} from "../production-task/task-service.service";
import {ActivatedRoute, Router, RouterLink} from "@angular/router";
import {FormsModule} from "@angular/forms";
import {MatFormField, MatInput, MatLabel} from "@angular/material/input";

@Component({
  selector: 'app-lost-files-recovery',
  imports: [
    JsonPipe,
    MatProgressSpinner,
    RucioDIDComponent,
    DatasetSizePipe,
    DatePipe,
    RucioURLPipe,
    MatButtonToggle,
    MatSlideToggle,
    MatButton,
    AsyncTaskProgressComponent,
    MatCard,
    MatCardTitle,
    MatCardContent,
    FormsModule,
    MatFormField,
    MatInput,
    MatLabel,
    RouterLink
  ],
  templateUrl: './lost-files-recovery.component.html',
  styleUrl: './lost-files-recovery.component.css'
})
export class LostFilesRecoveryComponent implements OnChanges {
    dataset = input<string>( );
    taskID = input<string|null>(null);

    // Writable value bound to the UI + URL, kept in sync with the dataset input.
    datasetValue = signal('');

    private readonly route = inject(ActivatedRoute);
    private readonly router = inject(Router);
    asyncResult = signal('');
    actionSubmitting = signal(false);
    lostFileRecoveryService = inject(LostFileRecoveryService);
    taskService = inject(TaskService);
    fileRecoveryInfo = computed(() => this.lostFileRecoveryService.lostFileRecoveryInfoResource.value() );
    isLoading = this.lostFileRecoveryService.lostFileRecoveryInfoResource.isLoading();
    error = computed(() => this.lostFileRecoveryService.lostFileRecoveryInfoResource.error() as HttpErrorResponse | null);
    actionError = signal<string|null>(null);
    asyncTaskIDAction = signal<string|null>(null);
    asyncTaskID = computed(() => {
      if (this.asyncTaskIDAction()) {
        return this.asyncTaskIDAction();
      }
      if (this.fileRecoveryInfo()?.recoveryInfo) {
        return this.fileRecoveryInfo()?.recoveryInfo.async_task_id;
      } else {
        return null;
      }
    });
    errorMessage = computed(() => {
      if (this.error()) {
        return setErrorMessage(this.error());
      }else {
        return null;
      }
    });

    ngOnChanges(): void {
        this.lostFileRecoveryService.datasetName.set(this.dataset());
        this.datasetValue.set(this.dataset());
        this.lostFileRecoveryService.selectedTask.set(this.taskID() ?? '');
    }

    constructor() {

    }

  protected submitRecovery(dryRun: boolean, recreateParent: boolean, resurrectDatasets: boolean): void {
      this.actionSubmitting.set(true);
      this.taskService.submitRuleAction([this.dataset()], 'recovery_lost_files', `Lost file recovery${dryRun ? ' (dry run)' : ''}`,
      [dryRun, recreateParent, resurrectDatasets]).subscribe(
      (actionResult) => {
          this.actionSubmitting.set(false);
          if (actionResult.error) {
              this.actionError.set(`Error submitting lost file recovery: ${actionResult.error}`);
          } else{
            if (actionResult.async_id) {
              this.asyncTaskIDAction.set(actionResult.async_id);
            }
          }
      }
    );
  }

  protected asyncTaskFinished(asyncResult: AsyncProdTaskSplitStatus): void {
        if (asyncResult.status === 'SUCCESS') {
            console.log(asyncResult.result);
            this.asyncResult.set(asyncResult.result.toString());
        } else {
            console.error('Async task failed or was cancelled');
            console.log(asyncResult.result);
        }
  }

  protected readonly HTMLInputElement = HTMLInputElement;
}
