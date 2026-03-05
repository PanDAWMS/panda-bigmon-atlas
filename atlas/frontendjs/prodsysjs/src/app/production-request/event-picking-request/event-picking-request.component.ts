import {Component, computed, inject, input, OnDestroy, OnInit, signal} from '@angular/core';
import {EventPickingService} from "../event-picking-request-creation/event-picking.service";
import {toObservable} from "@angular/core/rxjs-interop";
import {setErrorMessage} from "../../dsid-info/dsid-info.service";
import {HttpErrorResponse} from "@angular/common/http";
import {switchMap, takeUntil} from "rxjs/operators";
import {interval, Subject} from "rxjs";
import {Router, RouterLink} from "@angular/router";
import {ProductionTaskTableComponent} from "../../production-task-table/production-task-table.component";
import {TasksManagementService} from "../../tasks-management/tasks-management.service";
import {ProductionTask} from "../production-request-models";
import {MatProgressSpinner} from "@angular/material/progress-spinner";

@Component({
  selector: 'app-event-picking-request',
  imports: [
    RouterLink,
    ProductionTaskTableComponent,
    MatProgressSpinner
  ],
  templateUrl: './event-picking-request.component.html',
  styleUrl: './event-picking-request.component.css'
})
export class EventPickingRequestComponent implements OnInit, OnDestroy{
        jira = input<string>('');
        jiraTicket$ = toObservable(this.jira);
        epService = inject(EventPickingService);

        epProgress = this.epService.EPProgressResource.value;

        isLoading = this.epService.EPProgressResource.isLoading;
        error = this.epService.EPProgressResource.error;
        submitStatus = signal('');
        route = inject(Router);
        errorMessage = computed(() => {
          if (this.error()) {
              return setErrorMessage(this.error() as HttpErrorResponse | null);
          } else {
              return null;
          }
          });

        epIsProgressing = computed(() => {
          const streamProcessingExists: string[] = [];
          if (!this.epProgress()?.productions){
            return true;
          }
          for (const processing of this.epProgress()?.productions || []) {
            if (processing.status === 'guid_search' || processing.status === 'preparing' ){
              return true;
            }
            streamProcessingExists.push(processing.stream);
          }
          for (const epRequest of this.epProgress()?.requests){
            if (streamProcessingExists.indexOf(epRequest.stream) === -1){
              return true;
            }
          }
          return false;
        });
        epIsProgressingObservable$ = toObservable(this.epIsProgressing);
        taskManagementService = inject(TasksManagementService);
        readyToSubmit = computed(() => {
          if (!this.epProgress()?.productions){
            return false;
          }
          const streamProcessingExists: string[] = [];
          let toSubmitExists = false;
          for (const processing of this.epProgress()?.productions || []) {
            if (processing.status === 'picked'){
              toSubmitExists = true;
            }
            if (processing.status === 'error' || processing.status === 'guid_search'){
              return false;
            }
            streamProcessingExists.push(processing.stream);
          }
          for (const epRequest of this.epProgress().requests){
            if (streamProcessingExists.indexOf(epRequest.stream) === -1){
              return false;
            }
          }
          return toSubmitExists;
        });
        tasks: ProductionTask[] = [];
        private destroy$ = new Subject<void>();


        ngOnInit(): void {
          this.jiraTicket$.subscribe(jira => {
            this.epService.jira.set(jira);
          });
          this.taskManagementService.getTasksByHashtag(this.jira(), 'jira').subscribe((tasks) => {
            this.tasks = tasks;
          });
                    // Poll every 10 seconds while epIsProgressing is true
          this.epIsProgressingObservable$.pipe(
            switchMap(isProgressing => {
              if (isProgressing) {
                // Start polling every 10 seconds
                return interval(10000);
              } else {
                // Stop polling
                return [];
              }
            }),
            takeUntil(this.destroy$)
          ).subscribe(() => {
            // Reload the resource
            this.epService.EPProgressResource.reload();
          });
        }

        ngOnDestroy(): void {
          this.destroy$.next();
          this.destroy$.complete();
        }

        getStatusColor(status: string): string {
          switch (status) {
            case 'finished':
              return 'bg-orange-600';
            case 'done':
              return 'bg-green-600';
            case 'running':
              return 'bg-green-400';
            case 'picked':
              return 'bg-violet-600';
            case 'guid_search':
              return 'bg-yellow-600';
            case 'error':
              return 'bg-red-600';
            default:
              return 'bg-gray-600';
          }
        }
        constructor() {
        }

  protected submitEPRequests(): void {
      this.epService.submitEPRequest(this.jira()).subscribe({
        next: (response) => {
          this.submitStatus.set(response);
          this.epService.EPProgressResource.reload();
        },
        error: (err) => {
          this.submitStatus.set(setErrorMessage(err));
        }
      });
  }

  protected retryEPProgress(id: number): void {
      this.epService.retryEPProces(id).subscribe({
        next: (response) => {
          this.submitStatus.set(response);
          this.epService.EPProgressResource.reload();
        },
        error: (err) => {
          this.submitStatus.set(setErrorMessage(err));
        }
      });
  }
}
