import {Component, Input, OnInit} from '@angular/core';
import {StagingProgress} from "../../production-request/production-request-models";

@Component({
  selector: 'app-task-staging-progress',
  templateUrl: './task-staging-progress.component.html',
  styleUrls: ['./task-staging-progress.component.css']
})
export class TaskStagingProgressComponent implements OnInit {
  @Input() stagingProgress: StagingProgress;
  constructor() { }

  ngOnInit(): void {
  }

}
