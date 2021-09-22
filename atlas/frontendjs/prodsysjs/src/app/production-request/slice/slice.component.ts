import {ChangeDetectionStrategy, Component, Inject, Input, OnInit} from '@angular/core';
import {Slice, Step} from "../production-request-models";
import {MAT_DIALOG_DATA, MatDialog} from "@angular/material/dialog";

@Component({
  selector: 'app-slice',
  templateUrl: './slice.component.html',
  styleUrls: ['./slice.component.css'],
  changeDetection: ChangeDetectionStrategy.OnPush,

})




export class SliceComponent implements OnInit {
  @Input() slice: Slice;
  @Input() stepsOrder: string[];
  @Input() colorSchema: {[index: number]: any};

  @Input() steps: Step[];
  showFull = false;
  stepsInOrder: (Step|undefined)[] = [];
  checked = true;
  constructor(public dialog: MatDialog) { }
    ngOnInit(): void {
      if (this.stepsOrder !== undefined){
        this.stepsInOrder = [];
        let index = 0;
        for (let j = 0; j <= this.stepsOrder.length; j++){
          if ((index < this.steps.length) && (this.steps[index].step_name === this.stepsOrder[j])){
            this.stepsInOrder.push(this.steps[index]);
            index += 1;
          } else {
            this.stepsInOrder.push(undefined);
          }
        }
      } else {
        this.stepsInOrder = this.steps;
      }
  }
  openDialog(): void {
    this.dialog.open(SliceDetailsDialogComponent, {data: this.slice});
  }

}

@Component( {
  selector: 'app-slice-details-dialog',
  templateUrl: 'slice-details-dialog.html'
  })
export class SliceDetailsDialogComponent {
  constructor(@Inject(MAT_DIALOG_DATA) public slice: Slice) {
  }

}
