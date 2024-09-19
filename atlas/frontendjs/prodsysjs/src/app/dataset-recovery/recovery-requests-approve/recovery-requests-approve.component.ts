import {Component, computed, effect, inject, Signal, ViewChild} from '@angular/core';
import {AgGridAngular} from "ag-grid-angular";
import {TaskStatsComponent} from "../../production-request/task-stats/task-stats.component";
import {ReactiveFormsModule, UntypedFormControl} from "@angular/forms";
import {GridOptions, RowNode, SelectionChangedEvent} from "ag-grid-community";
import {Dataset, DatasetRecoveryService, DatasetRequest} from "../dataset-recovery.service";
import {convertBytes} from "../../derivation-exclusion/dataset-size.pipe";
import {toSignal} from "@angular/core/rxjs-interop";
import {MatButton} from "@angular/material/button";
import {NgClass} from "@angular/common";
import {BPTaskComponent} from "../../common/bptask/bptask.component";
import {RucioURLPipe} from "../../derivation-exclusion/rucio-url.pipe";

@Component({
  selector: 'app-recovery-requests-approve',
  standalone: true,
  imports: [
    AgGridAngular,
    TaskStatsComponent,
    ReactiveFormsModule,
    MatButton,
    NgClass,
    BPTaskComponent,
    RucioURLPipe
  ],
  templateUrl: './recovery-requests-approve.component.html',
  styleUrl: './recovery-requests-approve.component.css'
})
export class RecoveryRequestsApproveComponent {
  @ViewChild('agGrid') datasetsGrid!: AgGridAngular;
  public sitesControl = new UntypedFormControl([]);
  public sites: {[status: string]: number} = {};
  public sitesOrder: string[] = [];
  public gridOptions: GridOptions = {
    isExternalFilterPresent: this.isExternalFilterPresent.bind(this),
    doesExternalFilterPass: this.doesExternalFilterPass.bind(this)
  };
  datasetRecoveryService = inject(DatasetRecoveryService);
  allRequests = toSignal(this.datasetRecoveryService.getAllRequests());
  pendingRequests = computed(() => this.allRequests()?.filter(request => request.status === 'pending'));
  runningRequests: Signal<DatasetRequest[]> = computed(() => this.allRequests()?.filter(request => (request.status === 'running' || request.status === 'submitted')));
    columnDefs = [
    {
      field: 'original_task',
      headerName: 'Task',
      cellRenderer: params => {
        if (params.value === null) {
          return '';
        }
        return `<a href="https://bigpanda.cern.ch/task/${params.value}" >${params.value}</a>`;
      },
      suppressMenu: true,
      sort: 'desc',
            maxWidth: 110
    },
    {
      field: 'status',
      headerName: 'Status',
      cellRenderer: params => {
        switch (params.value) {
          case 'pending':
            return '<span style="color: orange;">Pending</span>';
          case 'submitted':
            return '<span style="color: green;">Submitted</span>';
          case 'running':
            return '<span style="color: blue;">Running</span>';
          case 'done':
            return '<span style="color: green;">Done</span>';
          default:
            return '';
        }
      },
      suppressMenu: true,
            maxWidth: 100
    },
    {headerName: 'Dataset Name', field: 'original_dataset', sortable: true, filter: false, resizable: false, flex: 3,
              cellRenderer: params => {
      // shorten name to 100 and put ... at the end
      return params.value.length > 100 ? params.value.substring(0, 100) + '...' : params.value;
      },
      tooltipField: 'original_dataset',
      },
    {headerName: 'Size', field: 'size', sortable: true, filter: false, resizable: false,
          cellRenderer: params => {
        return convertBytes(params.value);
      },
      maxWidth: 90},
    {headerName: 'Replicas', field: 'sites', sortable: true, filter: false, resizable: false,
      cellRenderer: params => {
      // replace _DATADISK with empty string on each value inside params array
      return params.value.split(',').map(replica => replica.replace('_DATADISK', '')).join(',');
      },
      flex: 1},
      {headerName: 'Requestor', field: 'requestor', sortable: true, filter: false, resizable: false,
      flex: 1},

  ];
    error = this.datasetRecoveryService.state.error;
    submitted = this.datasetRecoveryService.state.submitted;
    submitting = this.datasetRecoveryService.state.submitting;
  selectedDatasets: DatasetRequest[] = [];
  constructor() {
    effect(() => {
      this.selectedDatasets = [];
      if (this.pendingRequests() !== undefined) {
        for (const datasetInfo of this.pendingRequests()) {
          for (const replica of datasetInfo.sites.split(',')) {
            if (this.sites[replica] === undefined) {
              this.sites[replica] = 1;
            } else {
              this.sites[replica] += 1;
            }
          }
        }
        this.sitesOrder = Object.keys(this.sites).sort((a, b) => this.sites[b] - this.sites[a]);
        this.sitesControl = new UntypedFormControl(this.sitesOrder);
        this.sitesControl.valueChanges.subscribe(value => {
          this.datasetsGrid.api.onFilterChanged();
        });
      }
    });
  }

  isExternalFilterPresent(): boolean {
    return true;
  }

  selectFiltered(): void {
    this.datasetsGrid.api.deselectAll();
    this.datasetsGrid.api.selectAllFiltered();
  }

  doesExternalFilterPass(node: RowNode<DatasetRequest>): boolean {
    if (this.sitesControl.value.length === 0) {
      return true;
    }
    for (const replica of node.data.sites.split(',')) {
      if (this.sitesControl.value.includes(replica)) {
        return true;
      }
    }
    return false;
  }

  onSelectionChanged($event: SelectionChangedEvent<any>): void {
    this.selectedDatasets = $event.api.getSelectedRows();
  }

  submitRequests(): void {
    this.datasetRecoveryService.submitRecovery(this.selectedDatasets.map(dataset => dataset.id));
  }

  statusClass(status: string): string {
    switch (status) {
      case 'pending':
        return 'text-yellow-500';
      case 'submitted':
        return 'text-blue-500';
      case 'running':
        return 'text-indigo-500';
      case 'done':
        return 'text-green-500';
      default:
        return 'text-gray-500';
    }
  }
}

