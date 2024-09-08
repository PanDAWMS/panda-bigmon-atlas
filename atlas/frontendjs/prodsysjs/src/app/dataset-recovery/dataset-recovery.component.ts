import {Component, computed, effect, inject, Input, Signal, ViewChild} from '@angular/core';
import {Dataset, DatasetRecoveryService, TSData} from "./dataset-recovery.service";
import {MatProgressSpinner} from "@angular/material/progress-spinner";
import {AsyncPipe, DatePipe, JsonPipe, NgClass} from "@angular/common";
import {AgGridAngular} from "ag-grid-angular";
import {GridOptions, GridReadyEvent, RowNode, SelectionChangedEvent} from "ag-grid-community";
import {convertBytes} from "../derivation-exclusion/dataset-size.pipe";
import {FormsModule, ReactiveFormsModule, UntypedFormControl} from "@angular/forms";
import {TaskStatsComponent} from "../production-request/task-stats/task-stats.component";
import {MatButton} from "@angular/material/button";
import {MatFormField, MatLabel} from "@angular/material/form-field";
import {MatInput} from "@angular/material/input";
import {ActivatedRoute, Router, RouterLink} from "@angular/router";
import {MatIcon} from "@angular/material/icon";

@Component({
  selector: 'app-dataset-recovery',
  standalone: true,
  imports: [
    MatProgressSpinner,
    JsonPipe,
    NgClass,
    DatePipe,
    AgGridAngular,
    AsyncPipe,
    TaskStatsComponent,
    ReactiveFormsModule,
    MatButton,
    MatFormField,
    MatInput,
    MatLabel,
    FormsModule,
    RouterLink,
    MatIcon
  ],
  templateUrl: './dataset-recovery.component.html',
  styleUrl: './dataset-recovery.component.css'
})
export class DatasetRecoveryComponent {

  // @Input() set username(value: string) {
  //   this.datasetRecoveryService.setUsername(value);
  // }
  router = inject(Router);
  route = inject(ActivatedRoute);
  @ViewChild('agGrid') datasetsGrid!: AgGridAngular;

  public comment = '';
  public sitesControl = new UntypedFormControl([]);
  public sites: {[status: string]: number} = {};
  public sitesOrder: string[] = [];
  public gridOptions: GridOptions = {
    isExternalFilterPresent: this.isExternalFilterPresent.bind(this),
    doesExternalFilterPass: this.doesExternalFilterPass.bind(this)
  };
 datasetRecoveryService = inject(DatasetRecoveryService);
  public username = '';
  public taskID = '';
  public dataset = '';
  isLoading = this.datasetRecoveryService.state.isLoading;
  error = this.datasetRecoveryService.state.error;
  tsData: Signal<TSData> = this.datasetRecoveryService.state.tsData;
  submitted = this.datasetRecoveryService.state.submitted;
  submitting = this.datasetRecoveryService.state.submitting;
  columnDefs = [
    {
      field: 'task_id',
      headerName: 'Task',
      cellRenderer: params => {
        return `<a href="https://bigpanda.cern.ch/task/${params.value}" >${params.value}</a>`;
      },
      suppressMenu: true,
      sort: 'desc',
            maxWidth: 110
    },
    {
      field: 'status',
      headerName: 'S',
      cellRenderer: params => {
        switch (params.value) {
          case 'unavailable':
            return '<span style="color: gray">O</span>';
          case 'pending':
            return '<span style="color: orange;">⌛</span>';
          case 'submitted':
            return '<span style="color: green;">✔</span>';
          default:
            return '';

        }
      },
      suppressMenu: true,
            maxWidth: 20
    },
    {headerName: 'Dataset Name', field: 'input_dataset', sortable: true, filter: false, resizable: false, flex: 3,
              cellRenderer: params => {
      // shorten name to 100 and put ... at the end
      return params.value.length > 100 ? params.value.substring(0, 100) + '...' : params.value;
      },
      tooltipField: 'input_dataset',
      },
    {headerName: 'Size', field: 'size', sortable: true, filter: false, resizable: false,
          cellRenderer: params => {
        return convertBytes(params.value);
      },
      maxWidth: 100},
    {headerName: 'Replicas', field: 'replicas', sortable: true, filter: false, resizable: false,
      cellRenderer: params => {
      // replace _DATADISK with empty string on each value inside params array
      return params.value.map(replica => replica.replace('_DATADISK', '')).join(',');
      },
      flex: 1},

  ];
  selectedDatasets: Dataset[] = [];
   message: string;
constructor() {
  effect(() => {
    if (this.tsData()?.datasets !== undefined) {
      this.sites = {};
      this.sitesOrder = [];
      for (const datasetInfo of this.tsData().datasets) {
        for (const replica of datasetInfo.replicas) {
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
      } );
    }
  });
  this.route.queryParamMap.subscribe(queryParamMap => {
    if (queryParamMap.get('dataset')){
      this.datasetRecoveryService.setInputValues('', queryParamMap.get('dataset') , '');
      this.message = `Dataset: ${queryParamMap.get('dataset')}`;
      this.dataset = queryParamMap.get('dataset');
      this.username = '';
      this.taskID = '';
    } else if (queryParamMap.get('taskID')) {
      this.datasetRecoveryService.setInputValues('', '',  queryParamMap.get('taskID'));
      this.message = `Unavailable datasets for the task ${queryParamMap.get('taskID')}`;
      this.dataset = '';
      this.username = '';
      this.taskID = queryParamMap.get('taskID');
    } else if (queryParamMap.get('username')) {
      this.datasetRecoveryService.setInputValues(queryParamMap.get('username'), '' , '');
      this.message = `Unavailable datasets for the pending ${queryParamMap.get('username')} tasks`;
      this.dataset = '';
      this.username = queryParamMap.get('username');
      this.taskID = '';
    }
  });
}
  submitRequests(): void {
    this.datasetRecoveryService.submit(this.datasetsGrid.api.getSelectedRows(), this.comment);
  }

  onGridReady($event: GridReadyEvent<Dataset>) {

  }

  isExternalFilterPresent(): boolean {
    return true;
  }
  selectFiltered(): void {
    this.datasetsGrid.api.deselectAll();
    this.datasetsGrid.api.selectAllFiltered();
  }

  doesExternalFilterPass(node: RowNode<Dataset>): boolean {
    if (this.sitesControl.value.length === 0) {
      return true;
    }
    for (const replica of node.data.replicas) {
      if (this.sitesControl.value.includes(replica)) {
        return true;
      }
    }
    return false;
  }

  onSelectionChanged($event: SelectionChangedEvent<any>) {
    this.selectedDatasets = $event.api.getSelectedRows();
  }

}
