import { Component, OnInit } from '@angular/core';
import {AnalysisTasksService} from "../analysis-tasks.service";
import {ActivatedRoute, Router} from "@angular/router";
import {switchMap} from "rxjs/operators";
import {TasksManagementService} from "../../tasks-management/tasks-management.service";
import {AnalysisSlice} from "../analysis-task-model";
import {AgCellSliceComponent} from "../ag-cell-slice/ag-cell-slice.component";
@Component({
  selector: 'app-analysis-request',
  templateUrl: './analysis-request.component.html',
  styleUrls: ['./analysis-request.component.css']
})
export class AnalysisRequestComponent implements OnInit {
  public requestID = '';
  public slices: AnalysisSlice[] = [];
  public   requestInfo$ = this.route.paramMap.pipe(switchMap((params) => {
    this.requestID = params.get('id').toString();
    return this.taskManagementService.getProductionRequest(params.get('id'));
  }));
  public analysisSlices$ = this.route.paramMap.pipe(switchMap((params) => {
    return this.analysisTaskService.getAnalysisRequest(params.get('id').toString());
  }));
  sliceAGColumns = [
    {
      field: 'slice',
      headerName: 'Slice',
      cellRenderer: AgCellSliceComponent,
    }
  ];

  constructor(private route: ActivatedRoute, private analysisTaskService: AnalysisTasksService, private router: Router,
              private taskManagementService: TasksManagementService) { }

  ngOnInit(): void {
  }

}
