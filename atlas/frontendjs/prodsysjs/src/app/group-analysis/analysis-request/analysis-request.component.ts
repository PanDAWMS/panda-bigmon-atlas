import {Component, Inject, OnInit, ViewChild} from '@angular/core';
import {AnalysisTasksService} from "../analysis-tasks.service";
import {ActivatedRoute, Router} from "@angular/router";
import {catchError, map, mergeAll, switchMap, tap} from "rxjs/operators";
import {TasksManagementService} from "../../tasks-management/tasks-management.service";
import {AnalysisSlice} from "../analysis-task-model";
import {AgCellSliceComponent} from "../ag-cell-slice/ag-cell-slice.component";
import {FilterChangedEvent, GridOptions, GridReadyEvent, RowNode, SelectionChangedEvent} from "ag-grid-community";
import {AgGridAngular} from "ag-grid-angular";
import {ProductionTask} from "../../production-request/production-request-models";
import {combineLatest, of} from "rxjs";
import {MatLegacyTabChangeEvent as MatTabChangeEvent} from "@angular/material/legacy-tabs";
import {MAT_LEGACY_DIALOG_DATA as MAT_DIALOG_DATA, MatLegacyDialog as MatDialog, MatLegacyDialogRef as MatDialogRef} from "@angular/material/legacy-dialog";
@Component({
  selector: 'app-analysis-request',
  templateUrl: './analysis-request.component.html',
  styleUrls: ['./analysis-request.component.css']
})
export class AnalysisRequestComponent implements OnInit {
  public requestID = '';
  public slices: AnalysisSlice[] = [];
  public tasks: ProductionTask[] = [];
  public   requestInfo$ = this.route.paramMap.pipe(switchMap((params) => {
    this.requestID = params.get('id').toString();
    return this.taskManagementService.getProductionRequest(params.get('id'));
  }));
  @ViewChild('slicesGrid') slicesGrid!: AgGridAngular;
  public selectedSlices: AnalysisSlice[] = [];
  public selectedSlicesNumbers: number[] = [];
  public selectedTab  = 0;
  public taskLoadError?: string;
    public taskID: string|null = null;
    public datasetFilter = '';
  public gridOptions: GridOptions = {
    isExternalFilterPresent: this.isExternalFilterPresent.bind(this),
    doesExternalFilterPass: this.doesExternalFilterPass.bind(this)
  };
  public requestStats$ = this.route.paramMap.pipe(switchMap((params) => {
    return this.analysisTaskService.getAnalysisRequestStats(params.get('id').toString());
  }));
  public analysisSlices$ = this.route.paramMap.pipe(switchMap((params) => {
    return this.analysisTaskService.getAnalysisRequest(params.get('id').toString());
  }), //Sort slices by slice number
    map((slices) => slices.sort((a, b) => a.slice.slice - b.slice.slice)),
);
  public tasks$ =  this.route.paramMap.pipe(switchMap((params) => {

    this.requestID = params.get('id').toString();
    return this.taskManagementService.getTasksByRequestSlices(params.get('id'), null);
  }), catchError((err) => {
    this.taskLoadError = err.toString();
    return of([]);
  }));
  public selectSlicesOrTasks$ = combineLatest([this.analysisSlices$, this.tasks$]);
  sliceAGColumns = [
    {
      field: 'slice',
      headerName: '#',
      width: 90,
      checkboxSelection: true,
      cellRenderer: params => {
        return params.value.slice;
      }

    },

    {
      field: 'slice',
      headerName: 'Slice',
      flex: 1,
      cellRenderer: AgCellSliceComponent,
    }
  ];

  isExternalFilterPresent(): boolean {
    return true;
  }

  doesExternalFilterPass(node: RowNode<AnalysisSlice>): boolean {
    if (this.datasetFilter === '') {
      return true;
    } else {
      return node.data.slice.dataset.includes(this.datasetFilter);
    }
  }
  constructor(private route: ActivatedRoute, private analysisTaskService: AnalysisTasksService, private router: Router,
              private taskManagementService: TasksManagementService, public dialog: MatDialog) { }

  ngOnInit(): void {
    this.selectSlicesOrTasks$.subscribe(([slices, tasks]) => {
      this.slices = slices;
      let showTasks = true;
      for (const slice of this.slices) {
        if ((slice.steps.map(s => s.tasks.length)).reduce((sum, current) => sum + current, 0) === 0) {
          showTasks = false;
          break;
        }
      }
      if (showTasks) {
        this.selectedTab = 1;
      }
    });

  }
  onGridReady(params: GridReadyEvent<AnalysisSlice>): void {
    this.slicesGrid.api.selectAllFiltered();
  }
  onSelectionChanged($event: SelectionChangedEvent<any>): void {
     this.selectedSlices = this.slicesGrid.api.getSelectedRows();
     this.gridFilterOrSelectionChanged();

  }
  gridFilterOrSelectionChanged(): void {
      this.selectedSlicesNumbers = [];
      this.slicesGrid.api.forEachNodeAfterFilter((node) => {
         if (node.isSelected()) {
           this.selectedSlicesNumbers.push(node.data.slice.slice);
         }
       });
   }
  updateRequest(toUpdate: boolean): void {
    if (toUpdate) {
      this.analysisSlices$ = this.analysisTaskService.getAnalysisRequest(this.requestID).pipe(
        map((slices) => slices.sort((a, b) => a.slice.slice - b.slice.slice)
      ),
        tap( slices => {
            let showTasks = true;
            for (const slice of slices) {
              if ((slice.steps.map(s => s.tasks.length)).reduce((sum, current) => sum + current, 0) === 0) {
                showTasks = false;
                break;
              }
            }
            if (showTasks) {
              this.selectedTab = 1;
            }
        })
      );
      this.tasks$ = this.taskManagementService.getTasksByRequestSlices(this.requestID, null);
    }
  }
  onTaskChosen(taskID: number): void {
    this.router.navigate(['.'],
        { queryParams: {task: taskID}, queryParamsHandling: 'merge' , relativeTo: this.route });
  }

  filterChanged($event: FilterChangedEvent<any>): void {
     this.gridFilterOrSelectionChanged();
  }

  tabChanged($event: MatTabChangeEvent): void {
    this.selectedTab = $event.index;
  }

  showOutputs(): void {
    this.analysisTaskService.getAnalysisRequestOutputs(this.requestID).subscribe((outputs) => {
      this.dialog.open(DialogRequestOutputsComponent, {width: '90%', data: outputs});
    });
  }
}

// Dialog to show requests outputs
@Component({
  selector: 'app-dialog-request-outputs',
  templateUrl: './request-outputs.component.html',
})
export class DialogRequestOutputsComponent implements OnInit {
  outputs: string[] = [];
  constructor(@Inject(MAT_DIALOG_DATA) public data: string[], public dialogRef: MatDialogRef<DialogRequestOutputsComponent>) { }
  ngOnInit(): void {
    this.outputs = this.data;
  }
}
