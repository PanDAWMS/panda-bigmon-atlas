import {ChangeDetectionStrategy, Component, EventEmitter, Inject, Input, OnInit, Output} from '@angular/core';
import {ProductionTask, Slice, SliceBase, Step} from "../production-request-models";
import {MAT_LEGACY_DIALOG_DATA as MAT_DIALOG_DATA, MatLegacyDialog as MatDialog} from "@angular/material/legacy-dialog";
import {UntypedFormBuilder} from "@angular/forms";
import {ProductionRequestService} from "../production-request.service";

interface PreparedStep extends Step {
  tasksWithColor?: {tasks: number, color: string}[];
  tasksEvents?: number;
}


const COLOR_ORDER = [
  'darkgreen',
  'red',
  'blue',
  'lightgreen',
  'lightblue',
  'orange',
  'darkviolet',
  'black'
];

@Component({
  selector: 'app-slice',
  templateUrl: './slice.component.html',
  styleUrls: ['./slice.component.css', '../production-request.component.css'],
  changeDetection: ChangeDetectionStrategy.OnPush,

})

export class SliceComponent implements OnInit {
  @Input() slice: Slice;
  @Input() stepsOrder: string[];
  @Input() colorSchema: {[index: number]: any};
  @Output() tasksSelected = new EventEmitter<Step>();
  // @Input() steps: Step[];
  steps: Step[];
  showFull = false;
  stepsInOrder: (PreparedStep|undefined)[] = [];
  checked = true;
  constructor(public dialog: MatDialog) { }
    ngOnInit(): void {

      this.steps = [...this.slice.steps];
      if (this.stepsOrder !== undefined){
        this.stepsInOrder = [];
        let index = 0;
        for (let j = 0; j <= this.stepsOrder.length; j++){
          if ((index < this.steps.length) && (this.steps[index].step_name === this.stepsOrder[j])){
            const preparedStep = this.prepareStep(this.steps[index]);
            this.stepsInOrder.push(preparedStep);
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
    this.dialog.open(SliceDetailsDialogComponent, {data: {slice: this.slice,
         colorSchema: this.colorSchema}});
  }

  taskColor(taskStatus: string): string {
    let color: string;
    switch (taskStatus) {
        case 'finished':
        case 'done':
          color = 'darkgreen';
          break;
      case 'aborted':
      case 'broken':
      case 'failed':
        color = 'red';
        break;
      case 'running':
      case 'ready':
        color = 'lightgreen';
        break;
      case 'waiting':
      case 'submitting':
      case 'pending':
      case 'registered':
      case 'assigning':
        color = 'blue';
        break;
      case 'obsolete':
        color = 'lightblue';
        break;
      case 'exhausted':
      case 'paused':
        color = 'orange';
        break;
      case 'staging':
        color = 'darkviolet';
        break;
      default:
        color = 'black';
        break;
      }
    return color;
  }

  prepareStep(step: Step): PreparedStep {
    const preparedStep = step as PreparedStep;
    const currentColors: number[] = new Array(COLOR_ORDER.length).fill(0);
    let totalEvents = 0;
    for (const task of step.tasks){
      const color = this.taskColor(task.status);
      currentColors[COLOR_ORDER.indexOf(color)] += 1;
      totalEvents += task.total_events;
    }
    preparedStep.tasksEvents = totalEvents;
    preparedStep.tasksWithColor = [];
    for (let i = 0; i < COLOR_ORDER.length; i++){
      if (currentColors[i] > 0){
        preparedStep.tasksWithColor.push({tasks: currentColors[i], color: COLOR_ORDER[i]});
      }
    }
    return preparedStep;
  }


  selectTask(step: Step): void{
    this.tasksSelected.emit(step);
  }

}

// type SliceFields = "input_data" | "input_events" | "dataset" | "comment";

@Component( {
  selector: 'app-slice-details-dialog',
  templateUrl: 'slice-details-dialog.html',
  styleUrls: ['./slice.component.css', '../production-request.component.css'],
  })
export class SliceDetailsDialogComponent implements OnInit{
  panelOpenState: boolean;
  sliceForm = this.fb.group({
    input_data: [''],
    input_events: [''],
    dataset: [''],
    comment: ['']
  });
  originalSlice: Slice;
  slice: Slice;
  modifiedFields: Set<string> = new Set<string>();
  constructor(@Inject(MAT_DIALOG_DATA) public data: { slice: Slice,  colorSchema: any}, private fb: UntypedFormBuilder,
              private productionRequestService: ProductionRequestService) {

  }

  ngOnInit(): void {
    this.slice = {...this.data.slice};
    if (this.data.slice.modifiedFields !== undefined) {
      for (const [key, value] of Object.entries(this.data.slice.modifiedFields)){
        if (this.slice[key] !== value){
          this.slice[key] = value;
          this.modifiedFields.add(key);
        }
      }
    }
    this.sliceForm = this.fb.group(this.slice);
    this.sliceForm.valueChanges.subscribe( newValues => {
      const valueToPropagate: Slice = newValues;
      valueToPropagate.steps = [];
      for (const [key, value] of Object.entries(newValues)){
        if ((typeof this.data.slice[key] in ['string', 'number', 'boolean']) && (this.data.slice[key] === undefined) ||
          (this.data.slice[key] !== value)){
          this.modifiedFields.add(key);
        } else {
          this.modifiedFields.delete(key);
        }
      }
      this.productionRequestService.modifySlice(valueToPropagate);
      });
  }

  fieldChanged(fieldName: string): string {
    if (this.modifiedFields.has(fieldName)){
      return 'inputChanged';
    }
    return 'inputNotChanged';
  }

  saveSlice(): void {
    console.log('try to save slice');
    this.productionRequestService.saveSlice(this.sliceForm.value).subscribe(_ => this.modifiedFields = new Set<string>());
  }

}
