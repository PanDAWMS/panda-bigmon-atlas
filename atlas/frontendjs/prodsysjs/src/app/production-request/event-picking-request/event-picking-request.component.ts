import { Component, computed, inject, input, OnDestroy, OnInit, signal } from '@angular/core';
import {
  EventPickingService,
  ProducedDataset,
  ProducedDatasetsResponse
} from "../event-picking-request-creation/event-picking.service";
import {toObservable} from "@angular/core/rxjs-interop";
import {setErrorMessage} from "../../dsid-info/dsid-info.service";
import {HttpErrorResponse} from "@angular/common/http";
import {switchMap, takeUntil} from "rxjs/operators";
import {interval, of, Subject} from "rxjs";
import {Router, RouterLink} from "@angular/router";
import {ProductionTaskTableComponent} from "../../production-task-table/production-task-table.component";
import {TasksManagementService} from "../../tasks-management/tasks-management.service";
import {ProductionTask} from "../production-request-models";
import {MatProgressSpinner} from "@angular/material/progress-spinner";

import {MAT_DIALOG_DATA, MatDialog, MatDialogModule} from "@angular/material/dialog";
import {MatButton} from "@angular/material/button";
import {AgGridAngular} from "ag-grid-angular";
import {ColDef, SelectionChangedEvent} from "ag-grid-community";
import {
  MatAccordion,
  MatExpansionPanel, MatExpansionPanelDescription,
  MatExpansionPanelHeader,
  MatExpansionPanelTitle
} from "@angular/material/expansion";

@Component({
  selector: 'app-event-picking-request',
  imports: [
    RouterLink,
    ProductionTaskTableComponent,
    MatProgressSpinner,
    AgGridAngular,
    MatAccordion,
    MatExpansionPanel,
    MatExpansionPanelTitle,
    MatExpansionPanelHeader,
    MatExpansionPanelDescription
  ],
  templateUrl: './event-picking-request.component.html',
  styleUrl: './event-picking-request.component.css'
})
export class EventPickingRequestComponent implements OnInit, OnDestroy{
        private dialog = inject(MatDialog);


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
        epSomethingIsFinished = computed(() => {
          if (!this.epProgress()?.productions){
            return false;
          }
          for (const processing of this.epProgress()?.productions || []) {
            if (processing.status === 'done' || processing.status === 'finished') {
              return true;
            }
          }
          return false;
        });
        producedDatasets: ProducedDataset[] = [];
        existingContainers: string[] = [];
        finishedJira$ = toObservable(
          computed(() => this.epSomethingIsFinished() ? this.jira() : null)
        );


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
        selectedProducedDatasets: ProducedDataset[] = [];

        producedDatasetDefaultColDef: ColDef = {
          sortable: true,
          filter: true,
          resizable: true,
        };

        producedDatasetColumnDefs: ColDef[] = [
          {
            headerName: '',
            checkboxSelection: true,
            headerCheckboxSelection: true,
            width: 50,
            pinned: 'left',
            sortable: false,
            filter: false,
            resizable: false,
          },
          {
            field: 'name',
            headerName: 'Dataset',
            flex: 1,
            minWidth: 350,
          },
          {
            field: 'project',
            headerName: 'Project',
            width: 140,
          },
          {
            field: 'events',
            headerName: 'Events',
            width: 120,
          },
          {
            field: 'status',
            headerName: 'Status',
            width: 120,
          },
          {
            field: 'version',
            headerName: 'Version',
            width: 100,
          }
        ];

        containerPostfix = signal('');
        userScopes: string[] = [];
        selectedScope = signal('group.proj-evind.results');

        // Use full jira from backend when available (matches backend storage), fallback to route jira.
        jiraForContainer = computed(() => this.epProgress()?.requests?.[0]?.jira ?? this.jira());

        // Prefix uses jira key only, e.g. ATLPHYSVAL-1234
        jiraKey = computed(() => {
          const raw = this.jiraForContainer();
          return raw.includes('/') ? (raw.split('/').pop() ?? raw) : raw;
        });

        containerPrefix = computed(() => `${this.selectedScope()}.evntpick.`);

        containerName = computed(() => `${this.containerPrefix()}${this.containerPostfix().trim()}`);
        protected async copyDatasetNamesToClipboard(): Promise<void> {
          const text = this.producedDatasets.map(d => d.name).join('\n');
          if (!text) {
            this.submitStatus.set('No datasets to copy');
            return;
          }

          // Preferred path (secure context + permissions)
          try {
            if (navigator.clipboard && window.isSecureContext) {
              await navigator.clipboard.writeText(text);
              this.submitStatus.set(`Copied ${this.producedDatasets.length} dataset names to clipboard`);
              return;
            }
          } catch {
            // Fall through to legacy fallback
          }

          // Fallback for non-secure contexts (http) and restricted browsers
          const textarea = document.createElement('textarea');
          textarea.value = text;
          textarea.setAttribute('readonly', '');
          textarea.style.position = 'fixed';
          textarea.style.left = '-9999px';
          document.body.appendChild(textarea);
          textarea.select();
          textarea.setSelectionRange(0, textarea.value.length);

          try {
            const ok = document.execCommand('copy');
            this.submitStatus.set(
              ok
                ? `Copied ${this.producedDatasets.length} dataset names to clipboard`
                : 'Copy failed. Please copy manually.'
            );
          } catch {
            this.submitStatus.set('Copy failed. Please copy manually.');
          } finally {
            document.body.removeChild(textarea);
          }
        }
        protected createContainerForSelected(): void {
          const postfix = this.containerPostfix().trim();
          if (!postfix) {
            this.submitStatus.set('Container postfix is required');
            return;
          }

          const selectedNames = this.selectedProducedDatasets.map(d => d.name);
          if (selectedNames.length === 0) {
            this.submitStatus.set('Select at least one dataset');
            return;
          }

          this.epService.registerEPContainer(this.jiraForContainer(), this.containerName(), selectedNames).subscribe({
            next: (response) => {
              this.submitStatus.set(response);
              if (this.existingContainers.indexOf(this.containerName()) === -1) {
                this.existingContainers = [...this.existingContainers, this.containerName()];
              }
            },
            error: (err) => this.submitStatus.set(setErrorMessage(err))
          });
        }
        onProducedDatasetSelectionChanged(event: SelectionChangedEvent): void {
          this.selectedProducedDatasets = event.api.getSelectedRows() as ProducedDataset[];
        }
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
          this.finishedJira$.pipe(
            switchMap(jira => {
              if (!jira) {
                return of<ProducedDatasetsResponse>({datasets: [], containers: [], userScopes: []});
              }
              return this.epService.getProducedDatasets(jira);
            }),
            takeUntil(this.destroy$)
          ).subscribe({
            next: (response) => {
              this.producedDatasets = response.datasets || [];
              this.existingContainers = response.containers || [];
              this.userScopes = response.userScopes || ['group.proj-evind.results'];
              if (this.userScopes.length > 0) {
                this.selectedScope.set(this.userScopes[0]);
              }
            },
            error: (err) => {
              this.submitStatus.set(setErrorMessage(err));
              this.producedDatasets = [];
              this.existingContainers = [];
            }
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

  showContent(contentTyep: string, id: number): void{
          this.epService.getEPEvents(contentTyep, id.toString()).subscribe({
            next: (response) => {
              this.dialog.open(JsonDialogComponent, {
                    data: response,
                    width: '600px',
                    maxHeight: '80vh'
                  });
          },
          error: (err) => {
            this.submitStatus.set(setErrorMessage(err));
          }
      });
  }
}

@Component({
  selector: 'app-json-dialog',
  standalone: true,
  imports: [MatDialogModule, MatButton],
  template: `
    <div mat-dialog-title class="font-bold text-lg mb-4">
      Data Details
    </div>
    <div mat-dialog-content class="max-h-96 overflow-auto">
      <pre class="bg-gray-100 p-4 rounded text-sm overflow-auto">{{ jsonData }}</pre>
    </div>
    <div mat-dialog-actions align="end" class="mt-4">
      <button mat-button mat-dialog-close>Close</button>
    </div>
  `,
  styles: [`
    pre {
      font-family: 'Courier New', monospace;
      white-space: pre-wrap;
      word-wrap: break-word;
    }
  `]
})
export class JsonDialogComponent {
  data = inject(MAT_DIALOG_DATA);

  jsonData: string;

  constructor() {
    const data = this.data;

    this.jsonData = JSON.stringify(data, null, 2);
  }
}
