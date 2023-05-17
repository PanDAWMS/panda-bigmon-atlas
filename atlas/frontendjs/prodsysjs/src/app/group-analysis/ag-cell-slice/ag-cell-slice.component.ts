import {Component, Inject, Input, OnInit} from '@angular/core';
import {ICellRendererAngularComp} from "ag-grid-angular";
import {AnalysisSlice} from "../analysis-task-model";
import {ICellRendererParams} from "ag-grid-community";
import {MAT_DIALOG_DATA, MatDialog, MatDialogRef} from "@angular/material/dialog";

@Component({
  selector: 'app-ag-cell-slice',
  templateUrl: './ag-cell-slice.component.html',
  styleUrls: ['./ag-cell-slice.component.css']
})
export class AgCellSliceComponent implements ICellRendererAngularComp {
  public analysisSlice: AnalysisSlice;
  agInit(params: ICellRendererParams<AnalysisSlice, any>): void {
    this.analysisSlice = params.data;
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
  constructor(@Inject(MAT_DIALOG_DATA) public data: AnalysisSlice, public dialogRef: MatDialogRef<DialogSliceDetailsComponent>) { }
  ngOnInit(): void {
  }
}
