import {Component, EventEmitter, Inject, Input, OnInit, Output} from '@angular/core';
import {AnalysisTasksService} from "../analysis-tasks.service";
import {MAT_DIALOG_DATA, MatDialog, MatDialogRef} from "@angular/material/dialog";
import {AnalysisSlice, TaskTemplate, TemplateBase} from "../analysis-task-model";
import {PatternChanges} from "../pattern-edit/pattern-edit.component";
import {GridOptions, RowNode} from "ag-grid-community";
import {ProductionTask} from "../../production-request/production-request-models";

@Component({
  selector: 'app-analy-request-actions',
  templateUrl: './analy-request-actions.component.html',
  styleUrls: ['./analy-request-actions.component.css']
})
export class AnalyRequestActionsComponent implements OnInit {
  @Input() selectedSlices: number[] = [];
  @Input() productionRequestID: string;
  @Output() updateRequest = new EventEmitter<boolean>();
  public sendMessage = '';

  constructor(private analysisTaskService: AnalysisTasksService, public dialog: MatDialog) { }

  ngOnInit(): void {
  }

  executeAction(action: string): void {
    this.sendMessage = 'loading...';
    if (action === 'submit') {
      this.analysisTaskService.submitAnalysisRequestAction(this.productionRequestID, 'submit', this.selectedSlices).subscribe(
        (response) => {
          this.sendMessage = response.result;
          this.updateRequest.emit(true);
        }
      );
    } else if (action === 'clone') {
      this.analysisTaskService.submitAnalysisRequestAction(this.productionRequestID, 'clone', this.selectedSlices).subscribe(
        (response) => {
          this.sendMessage = response.result;
          this.updateRequest.emit(true);
        }
      );

    } else if (action === 'hide') {
      this.analysisTaskService.submitAnalysisRequestAction(this.productionRequestID, 'hide', this.selectedSlices).subscribe(
        (response) => {
          this.sendMessage = response.result;
          this.updateRequest.emit(true);
        }
      );
    } else if (action === 'modify') {
      this.analysisTaskService.getSlicesCommonTemplate(this.productionRequestID,  this.selectedSlices).subscribe(
        (response) => {
          if (response.error) {
            this.sendMessage = response.error;
            return;
          } else if (response.template === null) {
            this.sendMessage = 'No common template found';
            return;
          }
          this.dialog.open(DialogSliceModificationComponent, {data: {slices: response.slicesToModify.length, template: response.template}}).afterClosed(
            ).subscribe((result) => {
              if (result) {
                this.analysisTaskService.modifySlicesTemplate(this.productionRequestID, this.selectedSlices, result).subscribe(
                  (actionResponse) => {
                    this.sendMessage = actionResponse.result;
                    this.updateRequest.emit(true);
                  }
                );
              }
            }
          );

        }
      );
    }
  }
}


@Component({
  selector: 'app-dialog-slice-modification',
  templateUrl: './slice-modification.component.html',
})
export class DialogSliceModificationComponent implements OnInit {
  constructor(@Inject(MAT_DIALOG_DATA) public data: {slices: number, template: Partial<TaskTemplate>}, public dialogRef: MatDialogRef<DialogSliceModificationComponent>) { }
  currentTaskTemplate: Partial<TaskTemplate>;
  ngOnInit(): void {
    this.currentTaskTemplate = this.data.template;
  }

  saveChanges(data: PatternChanges): void {
    for (const key of data.removedFields){
      delete this.currentTaskTemplate[key];
    }
    for (const key of Object.keys(data.changes)){
      this.currentTaskTemplate[key] = data.changes[key];
    }
    this.dialogRef.close(this.currentTaskTemplate);
  }
}
