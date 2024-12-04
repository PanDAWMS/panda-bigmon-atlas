import {Component, Inject, Input, OnDestroy, OnInit} from '@angular/core';
import {ProductionTask} from '../production-request/production-request-models';
import {
  ActionParams,
  ReassignDestination, TaskAction,
  TaskActionResult,
  TaskService
} from '../production-task/task-service.service';
import {MAT_DIALOG_DATA, MatDialog, MatDialogRef} from '@angular/material/dialog';
import {Observable} from 'rxjs';
import {filter, map, switchMap, tap} from 'rxjs/operators';
import {MatSelectChange} from "@angular/material/select";





@Component({
  selector: 'app-task-action',
  templateUrl: './task-action.component.html',
  styleUrls: ['./task-action.component.css']
})

export class TaskActionComponent implements OnInit, OnDestroy {
  // @Input() task?: ProductionTask;
  @Input() tasks: ProductionTask[];
  @Input() active = true;

  reSendAction?: TaskAction = null;
  result?: TaskActionResult;

  summaryTasksResult: {status: string, result: string} = {status: '', result: ''};
  reassignEntities: ReassignDestination = {sites: [], nucleus: [], shares: []};
  siteOption = 'nokill';
  nucleuOption = 'nokill';
  shareOption = false;
  actionExecution$: Observable<{action: string, actions_result: string, tasks_result: {task_id: number, type: string, result: string}[]}>;
  actionExecuting = false;
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
    disable_idds: {name: 'Push staging rule', params_name: []},
    finish_plus_reload:  {name: 'Finish + reload', params_name: []},
    release_task: {name: 'Release', params_name: []},
    enable_job_cloning: {name: 'Enable job cloning', params_name: ['mode']},

  };
   SINGLE_TASK_CONFIRMATION_REQUIRED = [ 'abort', 'obsolete'];
   comment = '';
  selectedSites: string[] = [];
  jobCloningMode = 'runonce';

  constructor(private taskService: TaskService, public dialog: MatDialog) { }

  ngOnDestroy(): void {
    this.active = false;
  }

  ngOnInit(): void {
     this.taskService.getReassignEntities().subscribe( result => this.reassignEntities = result);
     this.actionExecution$ = this.taskService.getActionList().pipe(
       filter(value => (this.tasks.length > 0) && (value !== null) && (this.active)),
       tap(_ => {
                         this.actionExecuting = true;
                         this.summaryTasksResult = {status: '', result: ''};
                       }),
       switchMap((taskAction) => {
         this.reSendAction = {action: taskAction.action, tasks: [], comment: taskAction.comment, params: taskAction.params,
         action_name: this.TASKACTIONS[taskAction.action].name, params_name: this.TASKACTIONS[taskAction.action].params_name};
         return this.taskService.submitTaskAction(taskAction.tasks.map(task => task.id), taskAction.action,
           taskAction.comment, taskAction.params);
       }),
       tap(_ => this.actionExecuting = false),
       map(taskActionResult => {


         if (!taskActionResult.action_sent && taskActionResult.error && (taskActionResult.error !== '')){
               return {action: taskActionResult.action, actions_result: 'error',
                 tasks_result: [{task_id: taskActionResult.tasksID[0], type: 'error', result: taskActionResult.error}]};
         }

         if (this.tasks.length === 1){
           if (!taskActionResult.action_sent){
             if (taskActionResult.action_verification.length === 1){
               if (taskActionResult.action_verification[0].action_allowed &&
                 !taskActionResult.action_verification[0].user_allowed){
                 return {action: taskActionResult.action, actions_result: 'error',
                   tasks_result: [{task_id: taskActionResult.action_verification[0].id, type: 'error', result: 'User permission is insufficient to execute the action'}]};
               }
               return {action: taskActionResult.action, actions_result: 'error',
                  tasks_result: [{task_id: taskActionResult.action_verification[0].id, type: 'error', result:  `The action is not allowed for a task in ${this.tasks[0].status} status`}]};
             }
           }
           if (taskActionResult.result.length === 1) {
             if (taskActionResult.result[0].return_info !== null && taskActionResult.result[0].return_info.includes('Command rejected')){
               return {action: taskActionResult.action, actions_result: 'sent',
                 tasks_result: [{task_id: taskActionResult.result[0].task_id, type: 'warning', result:  `The command was sent to JEDI, return info:
              ${taskActionResult.result[0].return_info}; return code: ${taskActionResult.result[0].return_code};`}]};
             }
             return {action: taskActionResult.action, actions_result: 'sent',
                 tasks_result: [{task_id: taskActionResult.result[0].task_id, type: 'task_alt', result:  `The command was sent to JEDI, return info:
              ${taskActionResult.result[0].return_info}; return code: ${taskActionResult.result[0].return_code};`}]};
             // {action: taskActionResult.action, actions_result: 'task_alt', result: `The command was sent to JEDI, return info:
            //  ${taskActionResult.result[0].return_info}; return code: ${taskActionResult.result[0].return_code};`};
           }
         } else {
           const tasksResult = [];
           if (!taskActionResult.action_sent) {
             const reSendTasks = [];
             for (const actionVerification of taskActionResult.action_verification) {
               if (actionVerification.action_allowed && !actionVerification.user_allowed) {
                 tasksResult.push({
                   task_id: actionVerification.id,
                   type: 'error',
                   result: 'User permission is insufficient to execute the action'
                 });
               }
               if (!actionVerification.action_allowed) {
                 tasksResult.push({
                   task_id: actionVerification.id,
                   type: 'error',
                   result: `The action is not allowed for a task in this status`
                 });
               }
               if (actionVerification.action_allowed && actionVerification.user_allowed) {
                 tasksResult.push({
                   task_id: actionVerification.id,
                   type: 'task_alt',
                   result: `The action is allowed for a task`
                 });
                 reSendTasks.push(actionVerification.id);
               }
               if (reSendTasks.length > 0) {
                 this.reSendAction.tasks = this.tasks.filter(task => reSendTasks.includes(task.id));
               }
             }
             let returnString = `Error: commands were not sent to JEDI. ${tasksResult.length - reSendTasks.length} has problems`;
             if (reSendTasks.length > 0){
                returnString += `, for ${reSendTasks.length} tasks command can be sent again.`;
              }
             this.summaryTasksResult = {status: 'error', result: returnString};
             return {action: taskActionResult.action, actions_result: 'error', tasks_result: tasksResult};
           } else {
              let goodTasks = 0;
              let warningTasks = 0;
              for (const taskResult of taskActionResult.result) {
                if (taskResult.return_info !== null && taskResult.return_info.includes('Command rejected')){
                  tasksResult.push({
                     task_id: taskResult.task_id,
                     type: 'warning',
                     result: `The command was sent to JEDI, return info: ${taskResult.return_info}; return code: ${taskResult.return_code};`
                 });
                  warningTasks++;
                } else {
                  tasksResult.push({
                     task_id: taskResult.task_id,
                     type: 'task_alt',
                     result: `The command was sent to JEDI, return info: ${taskResult.return_info}; return code: ${taskResult.return_code};`
                 });
                  goodTasks++;
                }
              }
              if (warningTasks === 0){
              this.summaryTasksResult = {status: 'task_alt', result: `The commands were sent to JEDI`};
            } else {
              this.summaryTasksResult = {status: 'warning', result: `The commands were sent to JEDI, ${warningTasks} tasks have problems`};
              }
              return {action: taskActionResult.action, actions_result: 'sent', tasks_result: tasksResult};
           }

         }
       }
       ));
  }

  executeAction(action: string, params: ActionParams): void {
        if (  this.SINGLE_TASK_CONFIRMATION_REQUIRED.indexOf(action) > -1){
        this.dialog.open(DialogTaskSubmissionComponent, {data : {tasks: this.tasks, action, action_name: this.TASKACTIONS[action].name,
          params, params_name: this.TASKACTIONS[action].params_name, comment: this.comment}});
      } else {
        this.taskService.addAction({tasks: this.tasks, action, action_name: this.TASKACTIONS[action].name,
            params, params_name: this.TASKACTIONS[action].params_name, comment: this.comment});
      }

  }
  stopPropagation(event): void{
    event.stopPropagation();
  }


  summaryResult(actionResult: { action: string; actions_result: string; tasks_result: { task_id: number; type: string; result: string }[] }): string {

    if (actionResult.actions_result === 'error'){
      let returnString = `Error: commands were not sent to JEDI. ${actionResult.tasks_result.length - this.reSendAction.tasks.length} tasks has problems`;
      if (this.reSendAction.tasks.length > 0){
        returnString += `, for ${this.reSendAction.tasks.length} tasks command can be sent again.`;
      }
      return returnString;
    }
    if (actionResult.actions_result === 'sent'){
      let goodTasks = 0;
      let warningTasks = 0;
      for (const taskResult of actionResult.tasks_result){
        if (taskResult.type === 'task_alt'){
          goodTasks++;
        }
        if (taskResult.type === 'warning'){
          warningTasks++;
        }
      }
      return `Commands sent to JEDI. ${goodTasks} tasks were affected, ${warningTasks} tasks were not affected.`;
    }

  }

  reSendTasks(): void{
    this.taskService.addAction(this.reSendAction);
  }

  emptySelectedList(matSelectChange: MatSelectChange): void {
    if ((matSelectChange.value.length > 1) && (matSelectChange.value as string[]).includes('')){
     this.selectedSites = [''];
    }
  }
}

@Component({
  selector: 'app-dialog-task-action',
  templateUrl: 'dialog-task-action.html'
})
export class DialogTaskSubmissionComponent implements OnInit{

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
    this.taskService.addAction(this.data);
    this.dialogRef.close();
  }
}
