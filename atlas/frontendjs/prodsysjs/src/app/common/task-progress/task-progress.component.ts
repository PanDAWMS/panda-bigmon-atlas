import {Component, Input, OnChanges, OnInit, SimpleChanges} from '@angular/core';
import {ProductionTask} from "../../production-request/production-request-models";

@Component({
  selector: 'app-task-progress',
  templateUrl: './task-progress.component.html',
  styleUrls: ['./task-progress.component.css']
})
export class TaskProgressComponent implements OnChanges {
  @Input() task: ProductionTask;
  @Input() runningFiles: number;
  @Input() parentPercent: number;
  possibleStatus = ['done', 'running', 'failed', 'not_started', 'not_ready'];
  stats = {done: 0, running: 0, failed: 0, not_started: 0, not_ready: 0};
  statusDescription = {
    done: {color: 'green', descr: 'done', url: null},
    running: {color: 'lightgreen', descr: 'runnning', url: ''},
    failed: {color: 'red', descr: 'failed', url: ''},
    not_started: {color: 'grey', descr: 'Not yet defined', url: null},
    not_ready: {color: 'lightgrey', descr: 'Not yet done in parent(s)', url: null}

  }
  constructor() { }

  ngOnChanges(changes: SimpleChanges): void {

    if (this.task.total_files_finished === this.task.total_files_tobeused){
      this.stats.done = 1;
      this.stats.not_ready = 0;
      this.stats.running = 0;
      this.stats.failed = 0;
      this.stats.not_started = 0;
    } else {
      let total: number = +this.task.total_files_tobeused;
      const finishedFiles: number = +this.task.total_files_finished;
      const failedFiles: number = +this.task.total_files_failed;
      if ((finishedFiles + failedFiles + this.runningFiles) > total){
        total = finishedFiles + failedFiles + this.runningFiles;
      }
      this.stats.not_ready = 1 - this.parentPercent;
      this.stats.done = finishedFiles * this.parentPercent / total;
      this.stats.running = this.runningFiles * this.parentPercent / total;
      this.stats.failed = failedFiles * this.parentPercent / total;
      this.stats.not_started = (total - finishedFiles - failedFiles - this.runningFiles) * this.parentPercent / total;
    }
    if (this.stats.running > 0){
      this.statusDescription.running.url = `https://bigpanda.cern.ch/jobs/?jeditaskid=${this.task.id}&jobstatus=defined|waiting|pending|assigned|throttled|activated|sent|starting|running|holding|transferring`;
    }
    if (this.stats.failed > 0){
      this.statusDescription.running.url = `https://bigpanda.cern.ch/errors/?jeditaskid=${this.task.id}`;
    }


  }

}
