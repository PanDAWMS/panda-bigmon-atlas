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
import {MatTabChangeEvent} from "@angular/material/tabs";
import {MAT_DIALOG_DATA, MatDialog, MatDialogRef} from "@angular/material/dialog";
import {TASKS_CONSTANTS} from "../../common/constants/tasks_constants";
import {FormControl} from "@angular/forms";
import {COMMA, ENTER} from "@angular/cdk/keycodes";
import {MatChipInputEvent} from "@angular/material/chips";



@Component({
  selector: 'app-analysis-request',
  templateUrl: './analysis-request.component.html',
  styleUrls: ['./analysis-request.component.css']
})
export class AnalysisRequestComponent implements OnInit {
    readonly separatorKeysCodes = [ENTER, COMMA] as const;

  public PRODUCTION_REQUEST_STATUS = ['waiting', 'working', 'monitoring', 'finished', 'cancelled'];
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
  public hashtags$ = this.route.paramMap.pipe(switchMap((params) => {
    return this.analysisTaskService.getAnalysisRequestHashtags(params.get('id').toString());
  } ));
  public taskLoadError?: string;
    public taskID: string|null = null;
    public datasetFilter = '';
  public   sliceTypes: {[status: string]: number} ;
  public taskForAllSlices = true;
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
    map( slices => {
      return this.updateSliceTypes(slices); }), tap( slices => {
            this.countSliceTypes(slices);
    })
);
  public tasks$ =  this.route.paramMap.pipe(switchMap((params) => {

    this.requestID = params.get('id').toString();
    return this.taskManagementService.getTasksByRequestSlices(params.get('id'), null);
  }), catchError((err) => {
    this.taskLoadError = err.toString();
    return of([]);
  }));
 // public selectSlicesOrTasks$ = combineLatest([this.analysisSlices$, this.tasks$]);
  public pageSize = 20;

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
  SLICE_TYPE_ORDER = ['Total', 'Ready', 'Submitted', 'Slice Error', 'Task Error'];
  sliceTypeControl: FormControl = new FormControl(['Total', 'Ready', 'Submitted', 'Slice Error', 'Task Error']);
  showStatusChange = false;
  public hashtagUpload = false;

  isExternalFilterPresent(): boolean {
    return true;
  }
  countSliceTypes(slices: AnalysisSlice[]): void{
    const newStatus = {Total: slices.length, Ready: 0, Submitted: 0, 'Slice Error': 0, 'Task Error': 0};
    for (const slice of slices){
        for (const status of slice.status){
          if (status in newStatus){
            newStatus[status] += 1;
          } else {
            newStatus[status] = 1;
          }
        }
      }
    if (slices.length === newStatus.Submitted){
      this.selectedTab = 1;
    }
    this.sliceTypes = {...newStatus};
  }
  updateSliceTypes(slices: AnalysisSlice[]): AnalysisSlice[] {
    let currentStatus: string;
    let taskError = false;
    for (const slice of slices) {
          slice.status = [];
          if (slice.slice_error && slice.slice_error !== ''){
            slice.status.push('Slice Error');
          }
          currentStatus = 'Ready';
          taskError = false;
          for (const step of slice.steps) {
            if (step.step.status === 'Approved'){
              currentStatus = 'Submitted';
            }
            for (const task of step.tasks) {
              if (TASKS_CONSTANTS.BAD_TASKS_STATUS.includes(task.status)){
                taskError = true;
                break;
              }
            }
          }
          if (taskError){
            slice.status.push('Task Error');
          }
          slice.status.push(currentStatus);
    }
    return slices;
  }



  doesExternalFilterPass(node: RowNode<AnalysisSlice>): boolean {
    const filterPassed = (this.datasetFilter === '')  ||  node.data.slice.dataset.includes(this.datasetFilter);

    const sliceTypePassed = this.sliceTypeControl.value.length === 0 ||
      this.sliceTypeControl.value.includes('Total') || node.data.status.filter((val1) => {
      return this.sliceTypeControl.value.find((val2) => val1 === val2);}).length > 0;
    return filterPassed && sliceTypePassed;
    }


  constructor(private route: ActivatedRoute, private analysisTaskService: AnalysisTasksService, private router: Router,
              private taskManagementService: TasksManagementService, public dialog: MatDialog) { }

  ngOnInit(): void {
    // this.selectSlicesOrTasks$.subscribe(([slices, tasks]) => {
    //   this.slices = slices;
    //   let showTasks = true;
    //   for (const slice of this.slices) {
    //     if ((slice.steps.map(s => s.tasks.length)).reduce((sum, current) => sum + current, 0) === 0) {
    //       showTasks = false;
    //       break;
    //     }
    //   }
    //   if (showTasks) {
    //     this.selectedTab = 1;
    //   }
    // });
    this.sliceTypeControl.valueChanges.subscribe((value) => {
      this.slicesGrid.api.onFilterChanged();
    });

  }
  onGridReady(params: GridReadyEvent<AnalysisSlice>): void {
    this.slicesGrid.api.selectAllFiltered();
  }
  onSelectionChanged($event: SelectionChangedEvent<any>): void {
     this.selectedSlices = this.slicesGrid.api.getSelectedRows();
     this.gridFilterOrSelectionChanged();

  }
  clearSelection(): void {
    this.slicesGrid.api.deselectAll();
  }
  selectFiltered(): void {
    this.slicesGrid.api.deselectAll();
    this.slicesGrid.api.selectAllFiltered();
  }
  onPageSizeChanged(): void {
    this.slicesGrid.api.paginationSetPageSize(this.pageSize);
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
      this.requestInfo$ = this.taskManagementService.getProductionRequest(this.requestID);
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
        }), map( slices => {
      return this.updateSliceTypes(slices); }), tap( slices => {
            this.countSliceTypes(slices);
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
    if ((this.selectedTab === 1) && !this.taskForAllSlices) {
      this.tasks$ = this.taskManagementService.getTasksByRequestSlices(this.requestID, this.selectedSlices.map(s => s.slice.slice));
      }
  }

  showOutputs(): void {
    this.analysisTaskService.getAnalysisRequestOutputs(this.requestID).subscribe((outputs) => {
      this.dialog.open(DialogRequestOutputsComponent, {width: '90%', data: outputs});
    });
  }

  updateRequestStatus(value: any): void {
    this.analysisTaskService.setProductionRequestStatus(this.requestID, value).subscribe(() => {
      this.showStatusChange = false;
      this.updateRequest(true);
    } );
  }


  removeHashtag(hashtag: string): void {
    this.hashtagUpload = true;
    this.analysisTaskService.removeAnalysisRequestHashtag(this.requestID, hashtag).subscribe(() => {
      this.hashtagUpload = false;
      this.hashtags$ = this.analysisTaskService.getAnalysisRequestHashtags(this.requestID);
    }
    );
  }

  clickHashtag(hashtag: string): void {
    this.router.navigate(['tasks-by-hashtags', '|' + hashtag]);
  }

  addHashtag($event: MatChipInputEvent): void {
    this.hashtagUpload = true;
    const input = $event.value;
    const value = input.replace(/[^a-zA-Z0-9]/g, '');
    console.log(value);
    if ((value || '').trim()) {
      this.analysisTaskService.addAnalysisRequestHashtag(this.requestID, value).subscribe(() => {
        this.hashtagUpload = false;
        $event.chipInput.clear();
        this.hashtags$ = this.analysisTaskService.getAnalysisRequestHashtags(this.requestID);
      });
    }
  }

  changeSelectAllTasks(): void {
    if (!this.taskForAllSlices) {
      this.tasks$ = this.taskManagementService.getTasksByRequestSlices(this.requestID, this.selectedSlices.map(s => s.slice.slice));
    } else {
      this.tasks$ = this.taskManagementService.getTasksByRequestSlices(this.requestID, null);
    }
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
