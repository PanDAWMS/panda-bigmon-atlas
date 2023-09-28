import {Component, Inject, Input, OnInit} from '@angular/core';
import {ICellRendererAngularComp} from "ag-grid-angular";
import {AnalysisSlice, TaskTemplate, TemplateBase} from "../analysis-task-model";
import {ICellRendererParams} from "ag-grid-community";
import {MAT_DIALOG_DATA, MatDialog, MatDialogRef} from "@angular/material/dialog";
import {PatternChanges} from "../pattern-edit/pattern-edit.component";
import {AnalysisTasksService} from "../analysis-tasks.service";
import {BehaviorSubject} from "rxjs";

@Component({
  selector: 'app-ag-cell-slice',
  templateUrl: './ag-cell-slice.component.html',
  styleUrls: ['./ag-cell-slice.component.css']
})
export class AgCellSliceComponent implements ICellRendererAngularComp {
  public analysisSlice: AnalysisSlice;
  currentStatus = '';
  public parentStep: {request: number, slice: number}|null = null;

  agInit(params: ICellRendererParams<AnalysisSlice, any>): void {
    this.analysisSlice = params.data;
    const prodStep = this.analysisSlice.steps[0].step;
    if ((prodStep.production_step_parent_id) && (prodStep.production_step_parent_id !== prodStep.id) ) {
      this.parentStep = {request: prodStep.production_step_parent_request_id, slice: prodStep.production_step_parent_slice};
    }
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
    this.dialog.open(DialogSliceDetailsComponent, {width: '90%', data: this.analysisSlice});
  }

  protected readonly parent = parent;
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
  previewTask = 'Loading...';
  public parentStep: {request: number, slice: number}|null = null;

  constructor(@Inject(MAT_DIALOG_DATA) public data: AnalysisSlice, public dialogRef: MatDialogRef<DialogSliceDetailsComponent>,
              private  analysisTaskService: AnalysisTasksService) { }
  ngOnInit(): void {
    this.dataset = this.data.slice.dataset;
    this.template = this.data.steps[0].analysis_step.step_parameters;
    const prodStep = this.data.steps[0].step;
    if ((prodStep.production_step_parent_id) && (prodStep.production_step_parent_id !== prodStep.id) ) {
      this.parentStep = {request: prodStep.production_step_parent_request_id, slice: prodStep.production_step_parent_slice};
    }
  }

  openPreview(): void {
    this.previewTask = 'Loading...';
    this.analysisTaskService.getAnalysisTaskPreview(this.data.slice.request, this.data.slice.slice.toString()).subscribe(
      (preview) => {
        this.previewTask = preview;
      }
    );
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
