import {Component, Input, OnInit} from '@angular/core';
import {Step} from "../production-request-models";

@Component({
  selector: 'app-step',
  templateUrl: './step.component.html',
  styleUrls: ['./step.component.css']
})
export class StepComponent implements OnInit {
  @Input() step: Step;
  @Input() colorSchema: {[index: number]: any};

  panelOpenState: boolean;

  constructor() { }
  ngOnInit(): void {

  }


}
