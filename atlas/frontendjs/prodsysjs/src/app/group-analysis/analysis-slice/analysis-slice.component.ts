import {Component, Input, OnInit} from '@angular/core';
import {AnalysisSlice, AnalysisStep} from "../analysis-task-model";

@Component({
  selector: 'app-analysis-slice',
  templateUrl: './analysis-slice.component.html',
  styleUrls: ['./analysis-slice.component.css']
})
export class AnalysisSliceComponent implements OnInit {

  @Input() slice: AnalysisSlice;
  steps: AnalysisStep[];
  constructor() { }

  ngOnInit(): void {
    this.steps = [...this.slice.steps];

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

  openDialog(): void {
    console.log('openDialog');
  }
}
