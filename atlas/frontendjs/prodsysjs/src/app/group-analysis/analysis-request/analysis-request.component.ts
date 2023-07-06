import {Component, OnInit, ViewChild} from '@angular/core';
import {AnalysisTasksService} from "../analysis-tasks.service";
import {ActivatedRoute, Router} from "@angular/router";
import {catchError, map, mergeAll, switchMap, tap} from "rxjs/operators";
import {TasksManagementService} from "../../tasks-management/tasks-management.service";
import {AnalysisSlice} from "../analysis-task-model";
import {AgCellSliceComponent} from "../ag-cell-slice/ag-cell-slice.component";
import {GridReadyEvent, SelectionChangedEvent} from "ag-grid-community";
import {AgGridAngular} from "ag-grid-angular";
import {ProductionTask} from "../../production-request/production-request-models";
import {combineLatest, of} from "rxjs";
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

  public analysisSlices$ = this.route.paramMap.pipe(switchMap((params) => {
    return this.analysisTaskService.getAnalysisRequest(params.get('id').toString());
  }), //Sort slices by slice number
    map((slices) => slices.sort((a, b) => a.slice.slice - b.slice.slice)),
    tap( _ => {
   console.log('Slices');
  }));
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

  constructor(private route: ActivatedRoute, private analysisTaskService: AnalysisTasksService, private router: Router,
              private taskManagementService: TasksManagementService) { }

  ngOnInit(): void {
    this.selectSlicesOrTasks$.subscribe(([slices, tasks]) => {
      this.slices = slices;
      let showTasks = true;
      for (const slice of this.slices) {
        if (slice.steps.map(s => s.tasks).length === 0) {
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
     this.selectedSlicesNumbers = this.selectedSlices.map(s => s.slice.slice);
  }

  updateRequest(toUpdate: boolean) {
    if (toUpdate) {
      this.analysisSlices$ = this.analysisTaskService.getAnalysisRequest(this.requestID);
    }
  }
  onTaskChosen(taskID: number): void {
    this.router.navigate(['.'],
        { queryParams: {task: taskID}, queryParamsHandling: 'merge' , relativeTo: this.route });
  }
}
