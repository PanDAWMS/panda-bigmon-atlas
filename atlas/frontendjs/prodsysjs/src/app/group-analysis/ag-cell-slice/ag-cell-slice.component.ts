import {Component, Inject, Input, OnInit} from '@angular/core';
import {ICellRendererAngularComp} from "ag-grid-angular";
import {AnalysisSlice, TaskTemplate, TemplateBase} from "../analysis-task-model";
import {ICellRendererParams} from "ag-grid-community";
import {MAT_DIALOG_DATA, MatDialog, MatDialogRef} from "@angular/material/dialog";
import {PatternChanges} from "../pattern-edit/pattern-edit.component";
import {AnalysisTasksService} from "../analysis-tasks.service";

@Component({
  selector: 'app-ag-cell-slice',
  templateUrl: './ag-cell-slice.component.html',
  styleUrls: ['./ag-cell-slice.component.css']
})
export class AgCellSliceComponent implements ICellRendererAngularComp {
  public analysisSlice: AnalysisSlice;
  currentStatus = '';
  agInit(params: ICellRendererParams<AnalysisSlice, any>): void {
    this.analysisSlice = params.data;
    this.currentStatus = 'Ready';
    for (const step of this.analysisSlice.steps) {
      if (step.step.status === 'Approved'){
        this.currentStatus = 'Submitted';
      }
    }
   }

    constructor(public dialog: MatDialog) {
    }

   refresh(params: ICellRendererParams): boolean {
       return true;
   }

  showSlice() {
    this.dialog.open(DialogSliceDetailsComponent, {data: this.analysisSlice});
  }
}

// Dialog to show slice details
@Component({
  selector: 'app-dialog-slice-details',
  templateUrl: './slice-details.component.html',
})
export class DialogSliceDetailsComponent implements OnInit {
  dataset: string;
  template: Partial<TaskTemplate>;
  sendMessage = '';
  constructor(@Inject(MAT_DIALOG_DATA) public data: AnalysisSlice, public dialogRef: MatDialogRef<DialogSliceDetailsComponent>,
              private  analysisTaskService: AnalysisTasksService) { }
  ngOnInit(): void {
    this.dataset = this.data.slice.dataset;
    this.template = this.data.steps[0].analysis_step.step_parameters;
  }

  saveChanges(data: PatternChanges|null): void {
    if (data !== null) {
      for (const key of data.removedFields) {
        delete this.template[key];
      }
      for (const key of Object.keys(data.changes)) {
        this.template[key] = data.changes[key];
      }
    }
    this.sendMessage = 'loading...';
    this.analysisTaskService.modifySlicesTemplate(this.data.slice.request, [this.data.slice.slice], this.template, this.dataset).subscribe(
                  (actionResponse) => {
                    this.sendMessage = actionResponse.result;
                  }
                );
  }
}
