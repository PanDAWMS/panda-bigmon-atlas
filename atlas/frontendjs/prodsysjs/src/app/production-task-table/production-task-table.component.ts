import {Component, EventEmitter, Inject, Input, OnChanges, OnDestroy, OnInit, Output, ViewChild} from '@angular/core';
import {ProductionTask} from "../production-request/production-request-models";
import {SelectionModel} from "@angular/cdk/collections";
import {AgGridAngular, ICellRendererAngularComp} from "ag-grid-angular";
import {
  GridOptions,
  GridReadyEvent,
  GridSizeChangedEvent,
  ICellRendererParams,
  RowNode,
  SelectionChangedEvent
} from "ag-grid-community";
import {ActivatedRoute, Router} from "@angular/router";
import * as path from "path";
import {TasksManagementComponent} from "../tasks-management/tasks-management.component";
import {APP_BASE_HREF} from "@angular/common";
import {UntypedFormControl} from "@angular/forms";
import {TASKS_CONSTANTS} from "../common/constants/tasks_constants";
import {MAT_DIALOG_DATA, MatDialog, MatDialogConfig, MatDialogRef} from "@angular/material/dialog";
import {TaskAction} from "../production-task/task-service.service";
import {BehaviorSubject, Subject} from "rxjs";
import {MatIcon} from "@angular/material/icon";
@Component({
  selector: 'app-production-task-table',
  templateUrl: './production-task-table.component.html',
  styleUrls: ['./production-task-table.component.css']
})
export class ProductionTaskTableComponent implements OnInit, OnChanges, OnDestroy {

  @Input() tasks: ProductionTask[];
  @Input() taskToShow?: string;
  @Output() taskChosen = new EventEmitter<number>();
  @Input() showOwner = false;
  public tasksStatus: {[status: string]: number} = {};
  public tasksSteps: {[status: string]: number} = {};
  public taskStatusControl = new UntypedFormControl([]);
  public taskStepControl = new UntypedFormControl([]);
  public gridOptions: GridOptions = {
    isExternalFilterPresent: this.isExternalFilterPresent.bind(this),
    doesExternalFilterPass: this.doesExternalFilterPass.bind(this)
  };
  public TaskStatusStep = TASKS_CONSTANTS;


    taskAGColumns = [
      {
        field: 'id',
        headerName: '',
        cellRenderer: BtnCellRenderer,
        cellRendererParams: {
           clicked: (field: number) => {
            this.showTask(field);
          }
        },
        maxWidth: 30,
        sortable: false,
      },
      {
        field: 'name',
        headerName: 'Name',
        suppressMenu: true,
        filter: 'agTextColumnFilter',
        floatingFilter: true,
        floatingFilterComponentParams: {
          suppressFilterButton: true,
        },
        checkboxSelection: true,
      },
    {
      field: 'id',
      headerName: 'ID',
      cellRenderer: params => {
      //  return `<a href="${this.router.createUrlTree([this.baseHref, 'task', params.value])}" >${params.value}</a>`;
        return `<a href="https://bigpanda.cern.ch/task/${params.value}" >${params.value}</a>`;
      },
      suppressMenu: true,
      filter: 'agTextColumnFilter',
      floatingFilter: true,
      floatingFilterComponentParams: {
        suppressFilterButton: true,
      },
            maxWidth: 90,

    },

    {
      field: 'status',
      headerName: 'Status',
      filter: true,
      suppressMenu: true,
      maxWidth: 88,
      cellClass: params => ['taskStatus', params.value],


    },

      {
      field: 'ami_tag',
      headerName: 'AMI',
      suppressMenu: true,
      filter: 'agTextColumnFilter',
      floatingFilter: true,
      floatingFilterComponentParams: {
        suppressFilterButton: true,
      },

    },
    {
      field: 'username',
      headerName: 'Owner',
      suppressMenu: true,
      filter: 'agTextColumnFilter',
      floatingFilter: true,
      floatingFilterComponentParams: {
        suppressFilterButton: true,
      },
      hide: true,
    },
    {
      field: 'request_id',
      headerName: 'ReqID',
      suppressMenu: true,
      filter: 'agTextColumnFilter',
      floatingFilter: true,
      floatingFilterComponentParams: {
        suppressFilterButton: true,
      },
      cellRenderer: params => {
        return `<a href="/prodtask/slice_by_task/${params.data.id}" >${params.value}</a>`;
      },
    },
    {
      field: 'priority',
      headerName: 'Priority',
    },
    {
      field: 'total_events',
      headerName: 'Events',
    },
    {
      field: 'failureRate',
      headerName: 'Fail%',
    },
    {
      field: 'step_name',
      headerName: 'Step',
    },

  ];
    @ViewChild('taskAGGrid') tasksGrid!: AgGridAngular;
  public defaultColDef = {
    sortable: true
  };
  public selectedTasks: ProductionTask[] = [];
  public pageSize = 20;
  public dialogRef: MatDialogRef<DialogTaskDetailsComponent, any>;
  constructor(private router: Router, private route: ActivatedRoute,  @Inject(APP_BASE_HREF) private baseHref: string,
              public dialog: MatDialog) {
  }



  ngOnInit(): void {

    this.initTaskAGGrid();

  }

  ngOnChanges(): void {
  }
  initTaskAGGrid(): void {
    for (const task of this.tasks) {
      if (this.tasksStatus[task.status]) {
        this.tasksStatus[task.status] += 1;
      } else {
        this.tasksStatus[task.status] = 1;
      }
      if (this.tasksSteps[task.step_name]) {
        this.tasksSteps[task.step_name] += 1;
      } else {
        this.tasksSteps[task.step_name] = 1;
      }
    }
    this.taskStepControl = new UntypedFormControl(Object.keys(this.tasksSteps));
    this.taskStatusControl = new UntypedFormControl(Object.keys(this.tasksStatus));
    this.taskStepControl.valueChanges.subscribe( newValues => {
          this.tasksGrid.api.onFilterChanged();
    });
    this.taskStatusControl.valueChanges.subscribe( newValues => {
          this.tasksGrid.api.onFilterChanged();
    });

  }

  ngOnDestroy(): void {
    if (this.dialogRef) {
      this.dialogRef.close();
    }
  }
  onGridReady(params: GridReadyEvent<ProductionTask>): void {
    if (this.showOwner) {
      params.columnApi.applyColumnState({
        state: [{colId: 'username', hide: false}],
      });
    }
    params.columnApi.autoSizeColumns(this.taskAGColumns.map( column => column.field), true);
    if (this.taskToShow && this.tasks.filter(task => task.id.toString() === this.taskToShow).length > 0){
       // console.log('showing task', this.taskToShow);
        this.showTask(parseInt(this.taskToShow), false);
    }
  }

  isExternalFilterPresent(): boolean {
    return this.taskStatusControl.value.length > 0;
  }

  doesExternalFilterPass(node: RowNode<ProductionTask>): boolean {
    const statusChecked = (this.taskStatusControl.value.length  === 0) || (this.taskStatusControl.value.includes(node.data.status));
    const stepChecked = (this.taskStepControl.value.length  === 0) || (this.taskStepControl.value.includes(node.data.step_name));
    return statusChecked && stepChecked;
  }

  onSelectionChanged($event: SelectionChangedEvent<any>): void {
     this.selectedTasks = this.tasksGrid.api.getSelectedRows();

  }

  selectFiltered(): void {
    this.tasksGrid.api.deselectAll();
    this.tasksGrid.api.selectAllFiltered();
  }

  clearSelection(): void {
    this.tasksGrid.api.deselectAll();
  }

  showTask(taskID: number, emit: boolean = true): void {
    const filteredTasks = [];
    this.tasksGrid.api.forEachNodeAfterFilterAndSort(task => filteredTasks.push(task.data.id));
    if (emit) {
      this.taskChosen.emit(taskID);
    }


    this.dialogRef = this.dialog.open(DialogTaskDetailsComponent,  {data: {selectedTask: taskID, filteredTasks}, closeOnNavigation: true});
    this.dialogRef.componentInstance.taskChosen.subscribe( newTask => {
      this.taskChosen.emit(newTask);
    });
    this.dialogRef.afterClosed().subscribe(result => {
      this.taskChosen.emit(null);
      this.dialogRef.componentInstance.taskChosen.unsubscribe();
    });
  }
  onPageSizeChanged(): void {
    this.tasksGrid.api.paginationSetPageSize(this.pageSize);
  }
}

// Dialog to show task details
@Component({
  selector: 'app-dialog-task-details',
  templateUrl: './task-details.component.html',
})
export class DialogTaskDetailsComponent implements OnInit {

  @Output() taskChosen = new EventEmitter<number>();

  constructor(@Inject(MAT_DIALOG_DATA) public data: {selectedTask: number, filteredTasks: number[]},
              public dialogRef: MatDialogRef<DialogTaskDetailsComponent>) { }
  currentTask: number;
  currentIndex: number;
  ngOnInit(): void {
    this.currentTask = this.data.selectedTask;
    this.currentIndex = this.data.filteredTasks.indexOf(this.currentTask);
  }

  nextTask(): void {
    const index = this.data.filteredTasks.indexOf(this.currentTask);
    if (index < this.data.filteredTasks.length - 1) {
      this.currentTask = this.data.filteredTasks[index + 1];
      this.currentIndex = this.data.filteredTasks.indexOf(this.currentTask);
      this.taskChosen.emit(this.currentTask);
    }
  }

  previousTask(): void {
    const index = this.data.filteredTasks.indexOf(this.currentTask);
    if (index > 0) {
      this.currentTask = this.data.filteredTasks[index - 1];
      this.currentIndex = this.data.filteredTasks.indexOf(this.currentTask);
      this.taskChosen.emit(this.currentTask);
    }
  }
}

@Component({
  selector: 'btn-cell-renderer',
  template: `
    <a  style="color: #43a047; cursor: pointer" (click)="btnClickedHandler()"><mat-icon class="more">
more
</mat-icon></a>
  `,
})
export class BtnCellRenderer implements ICellRendererAngularComp {
  private params: any;

  agInit(params: any): void {
    this.params = params;
  }

  btnClickedHandler(): void {
    this.params.clicked(this.params.value);
  }

  refresh(params: ICellRendererParams): boolean {
    return false;
  }
}