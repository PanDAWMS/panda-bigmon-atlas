import {Component, Input, OnInit} from '@angular/core';
import {AnalysisStep} from "../analysis-task-model";

@Component({
  selector: 'app-step-tasks',
  templateUrl: './step-tasks.component.html',
  styleUrls: ['./step-tasks.component.css']
})
export class StepTasksComponent implements OnInit {

  @Input() step: AnalysisStep;
  constructor() { }

  ngOnInit(): void {
  }

}
