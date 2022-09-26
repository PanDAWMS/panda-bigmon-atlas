import {Component, Inject, Input, OnInit} from '@angular/core';
import {ProductionTask} from '../production-request/production-request-models';
import {ReassignDestination, TaskActionResult, TaskService} from '../production-task/task-service.service';
import {MAT_DIALOG_DATA, MatDialog, MatDialogRef} from '@angular/material/dialog';

type ActionParams = number[]|string[]|boolean[]|null;

interface TaskAction {
  task: ProductionTask;
  action: string;
  action_name: string;
  params: ActionParams;
  params_name: string[]|null;
  comment: string;
}



@Component({
  selector: 'app-task-action',
  templateUrl: './task-action.component.html',
  styleUrls: ['./task-action.component.css']
})

export class TaskActionComponent implements OnInit {
  @Input() task: ProductionTask;
  result?: TaskActionResult;
  reassignEntities: ReassignDestination = {sites: [], nucleus: [], shares: []};
  siteOption = 'nokill';
  nucleuOption = 'nokill';
  shareOption = 'default';
  TASKACTIONS = {
    abort: {name: 'Abort', params_name: []},
    finish: {name: 'Finish', params_name: ['Soft Finish']},
    change_priority: {name: 'Change priority', params_name: ['priority']},
    reassign_to_site: {name: 'Reassign to site', params_name: ['site', 'mode']},
    reassign_to_cloud: {name: 'Reassign to cloud', params_name: ['cloud', 'mode']},
    reassign_to_nucleus: {name: 'Reassign to nucleus', params_name: ['nucleus', 'mode']},
    reassign_to_share: {name: 'Reassign to share', params_name: ['share', 'mode']},
    retry: {name: 'Retry', params_name: ['', '', 'Discard events', 'Discard iDDS']},
    change_ram_count: {name: 'Change ram count', params_name: ['value']},
    change_wall_time: {name: 'Change wall time', params_name: ['value']},
    change_cpu_time: {name: 'Change cpu time', params_name: ['value']},
    increase_attempt_number: {name: 'Increase attempts numbers', params_name: ['number']},
    abort_unfinished_jobs: {name: 'Abort jobs', params_name: ['mode']},
    delete_output: {name: 'Delete output', params_name: ['output']},
    kill_job: {name: 'Kill jobs', params_name: ['jobs']},
    obsolete: {name: 'Obsolete', params_name: []},
    change_core_count: {name: 'Change core count', params_name: ['cores']},
    change_split_rule: {name: 'Change split rule', params_name: ['parameter', 'value']},
    pause_task: {name: 'Pause', params_name: []},
    resume_task: {name: 'Resume', params_name: []},
    trigger_task: {name: 'Trigger', params_name: []},
    avalanche_task: {name: 'Avalanche', params_name: []},
    reload_input: {name: 'Reload input', params_name: []},
    retry_new: {name: 'Retry to a new task', params_name: []},
    set_hashtag: {name: 'Set hashtag', params_name: ['hashtag']},
    remove_hashtag: {name: 'Remove hashtag', params_name: ['hashtag']},
    sync_jedi: {name: 'Sync with JEDI', params_name: []},
    disable_idds: {name: 'Push staging rule', params_name: []}
  };
   SINGLE_TASK_CONFIRMATION_REQUIRED = [ 'abort', 'obsolete'];
   comment = '';

  constructor(private taskService: TaskService, public dialog: MatDialog) { }

  ngOnInit(): void {
     this.taskService.getReassignEntities().subscribe( result => this.reassignEntities = result);
  }

  executeAction(action: string, params: ActionParams): void {
    console.log(params);
    if (  this.SINGLE_TASK_CONFIRMATION_REQUIRED.indexOf(action) > -1){
      this.dialog.open(DialogTaskSubmissionComponent, {data : {task: this.task, action, action_name: this.TASKACTIONS[action].name,
        params, params_name: this.TASKACTIONS[action].params_name, comment: this.comment}});
    } else {
      this.taskService.submitTaskAction([this.task.id], action,  this.comment, params).
        subscribe(result => this.result = result);
    }

  }
  stopPropagation(event): void{
    event.stopPropagation();
  }

}

@Component({
  selector: 'app-dialog-task-action',
  templateUrl: 'dialog-task-action.html'
})
export class DialogTaskSubmissionComponent implements OnInit{

  actionResult?: TaskActionResult;
  parameters = '';
  comment = '';

  constructor(@Inject(MAT_DIALOG_DATA) public data: TaskAction, public dialogRef: MatDialogRef<DialogTaskSubmissionComponent>,
              private taskService: TaskService) {
  }

  ngOnInit(): void {
    this.comment = this.data.comment;
    this.parameters = ' Parameters: ';
    for (const paramIndex in this.data.params_name){
      if (this.data.params_name[paramIndex] !== ''){
        this.parameters += `${this.data.params_name[paramIndex]} : ${this.data.params[paramIndex].toString()};`;
      }
    }
  }

  submitAction(): void {
    this.taskService.submitTaskAction([this.data.task.id], this.data.action,  this.comment, this.data.params).
    subscribe(result => this.actionResult = result);
  }
}
