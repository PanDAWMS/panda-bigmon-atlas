import { Component, computed, effect, EventEmitter, inject, input, OnInit, Output, Signal, ViewChild } from '@angular/core';
import {DataCarouselService, DatasetExistsResponse} from '../../DataCarousel/data-carousel.service';
import {TaskActionLog, TaskService} from '../../production-task/task-service.service';
import {Observable} from 'rxjs';
import {toObservable} from '@angular/core/rxjs-interop';
import {observableToBeFn} from 'rxjs/internal/testing/TestScheduler';
import {DatePipe, JsonPipe} from '@angular/common';
import {HttpErrorResponse} from '@angular/common/http';
import {setErrorMessage} from '../../dsid-info/dsid-info.service';
import {MatCard, MatCardHeader, MatCardTitle} from '@angular/material/card';
import {RucioURLPipe} from '../../derivation-exclusion/rucio-url.pipe';
import {convertBytes, DatasetSizePipe} from '../../derivation-exclusion/dataset-size.pipe';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {AgGridAngular} from 'ag-grid-angular';
import {FilterChangedEvent, GridOptions, GridReadyEvent} from 'ag-grid-community';
import {BtnCellRenderer} from '../../production-task-table/production-task-table.component';
import {DialogDatasetDetailsComponent} from '../../DataCarousel/staging-management/staging-management.component';
import {
  MAT_DIALOG_DATA,
  MatDialog,
  MatDialogActions,
  MatDialogClose,
  MatDialogContent,
  MatDialogRef
} from '@angular/material/dialog';
import {MatButton} from '@angular/material/button';
import {RucioDIDComponent} from '../rucio-did/rucio-did.component';

@Component({
  selector: 'app-rucio-container',
  imports: [
    RucioURLPipe,
    DatasetSizePipe,
    DatePipe,
    MatProgressSpinner,
    AgGridAngular
  ],
  templateUrl: './rucio-container.component.html',
  styleUrl: './rucio-container.component.css'
})
export class RucioContainerComponent implements OnInit {
    dialog = inject(MatDialog);

    private dataCarouselService = inject(DataCarouselService);
    containerName = input<string>();
    containerName$ = toObservable(this.containerName);
    containerExists = computed(() => this.dataCarouselService.containerInfoResource.value()?.dataset_exists );
    containerInfo: Signal<DatasetExistsResponse|undefined> = computed(() => {
      if (this.containerExists()){
        return this.dataCarouselService.containerInfoResource.value().dataset_knowledge as DatasetExistsResponse;
      }else {
        return undefined;
      }
    });
    isLoading = this.dataCarouselService.containerInfoResource.isLoading;
    error = computed(() => this.dataCarouselService.containerInfoResource.error() as HttpErrorResponse | null);
    errorMessage = computed(() => {
      if (this.error()) {
        return setErrorMessage(this.error());
      }else {
        return null;
      }
    });

    public dialogRef: MatDialogRef<DialogDatasetInsideContainerDetailsComponent>;

    @ViewChild('agGrid') datasetsGrid!: AgGridAngular;
      columnDefs = [        {
      field: 'name',
        headerName: '',
        cellRenderer: BtnCellRenderer,
        cellRendererParams: {
           clicked: (field: string) => {
            this.showDataset(field);
          }
        },
        maxWidth: 30,
        sortable: false,
      },
    {field: 'name', headerName: 'Dataset',
    // split dataset name by '.' and display only first and second to last fields
    cellRenderer: params => {
      let dataset = params.value;
      if (dataset.indexOf(':') !== -1) {
        dataset = dataset.split(':')[1];
      }
      const parts = dataset.split('.');
      if (parts.length > 2) {
        if (parts[0] === 'user' || parts[0] === 'group') {
          return `<a href="https://rucio-ui.cern.ch/did?scope=${parts[0]}.${parts[1]}&name=${dataset}">${dataset}</a>`;
        }
        return `<a href="https://rucio-ui.cern.ch/did?scope=${parts[0]}&name=${dataset}">${dataset}</a>`;
      }
      return dataset;
      },
      filter: true,
      floatingFilter: true
    },
    {field: 'length', headerName: 'Files', maxWidth: 100,},
    {field: 'bytes', headerName: 'Size',maxWidth: 100,
      cellRenderer: params => {
        return convertBytes(params.value);
      }
    }];
      adjustColumns(params: FilterChangedEvent<any>) {
            params.api.sizeColumnsToFit();
      }
    ngOnInit(): void {
        this.containerName$.subscribe(containerName => {
          if (containerName){
            this.dataCarouselService.containerName.set(containerName);
          }else {
            this.dataCarouselService.containerName.set('');
          }
        });


    }

   showDataset(dataset: string): void {
    const filteredDatasets = [];
    this.datasetsGrid.api.forEachNodeAfterFilterAndSort((selectedRule) => {
        filteredDatasets.push(selectedRule.data.name);
    });
    this.dialogRef = this.dialog.open(DialogDatasetDetailsComponent,
      {data: {selectedDataset: dataset, filteredDatasets}, closeOnNavigation: true});
    this.dialogRef.afterClosed().subscribe(result => {
    });
  }
  onGridReady(params: GridReadyEvent<any>) {
    params.api.sizeColumnsToFit();
  }
}
@Component({
  selector: 'app-dialog-dataset-inside-container-details',
  templateUrl: './dataset-in-container-details.component.html',
  imports: [
    MatButton,
    MatDialogActions,
    MatDialogClose,
    MatDialogContent,
    RucioDIDComponent
  ],
  standalone: true
})
export class DialogDatasetInsideContainerDetailsComponent implements OnInit {
  data = inject<{
    selectedDataset: string;
    filteredDatasets: string[];
}>(MAT_DIALOG_DATA);
  dialogRef = inject<MatDialogRef<DialogDatasetInsideContainerDetailsComponent>>(MatDialogRef);


  @Output() datasetChosen = new EventEmitter<string>();
  currentDataset: string;
  currentIndex: number;
  ngOnInit(): void {
    this.currentDataset = this.data.selectedDataset;
    this.currentIndex = this.data.filteredDatasets.indexOf(this.currentDataset);
  }

  nextTask(): void {
    const index = this.data.filteredDatasets.indexOf(this.currentDataset);
    if (index < this.data.filteredDatasets.length - 1) {
      this.currentDataset = this.data.filteredDatasets[index + 1];
      this.currentIndex = this.data.filteredDatasets.indexOf(this.currentDataset);
      this.datasetChosen.emit(this.currentDataset);
    }
  }

  previousTask(): void {
    const index = this.data.filteredDatasets.indexOf(this.currentDataset);
    if (index > 0) {
      this.currentDataset = this.data.filteredDatasets[index - 1];
      this.currentIndex = this.data.filteredDatasets.indexOf(this.currentDataset);
      this.datasetChosen.emit(this.currentDataset);
    }
  }
}
