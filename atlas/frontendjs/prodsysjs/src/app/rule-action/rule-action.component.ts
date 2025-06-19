import {Component, Inject, Input, OnDestroy, OnInit} from '@angular/core';
import {
  ActionParams,
  RuleAction, RuleActionResult,
  TaskService
} from '../production-task/task-service.service';
import {
  MAT_DIALOG_DATA,
  MatDialog,
  MatDialogActions,
  MatDialogContent,
  MatDialogRef,
  MatDialogTitle
} from '@angular/material/dialog';
import {Observable} from 'rxjs';
import {filter, map, switchMap, tap} from 'rxjs/operators';
import {MatFormField} from '@angular/material/select';
import {MatMenu, MatMenuTrigger} from '@angular/material/menu';
import {MatButton} from '@angular/material/button';
import {MatDivider} from '@angular/material/divider';
import {MatCheckbox} from '@angular/material/checkbox';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';
import {MatProgressBar} from '@angular/material/progress-bar';
import {AsyncPipe, NgForOf, NgIf} from '@angular/common';
import {MatCard} from '@angular/material/card';
import {MatIcon} from '@angular/material/icon';
import {
  MatAccordion,
  MatExpansionPanel,
  MatExpansionPanelHeader,
  MatExpansionPanelTitle
} from '@angular/material/expansion';
import {MatList, MatListItem} from '@angular/material/list';
import {MatInput} from '@angular/material/input';
import {MatLabel} from '@angular/material/form-field';
import {AsyncTaskProgressComponent} from '../common/async-task-progress/async-task-progress.component';
import {AsyncProdTaskSplitStatus, RulesResultInfo} from '../production-request/production-request.service';



interface RuleResults {
    action: string;
    actions_result: string;
    datasets_result: {
      dataset: string;
      type: string;
      result: string;
    }[];
  }

@Component({
  selector: 'app-rule-action',
  templateUrl: './rule-action.component.html',
  styleUrls: ['./rule-action.component.css'],
  imports: [
    MatMenuTrigger,
    MatMenu,
    MatButton,
    MatDivider,
    MatCheckbox,
    MatFormField,
    FormsModule,
    MatProgressBar,
    AsyncPipe,
    MatCard,
    MatIcon,
    MatAccordion,
    MatExpansionPanelHeader,
    MatExpansionPanel,
    MatExpansionPanelTitle,
    MatLabel,
    MatList,
    MatListItem,
    MatInput,
    NgIf,
    NgForOf,
    AsyncTaskProgressComponent
  ],
  standalone: true
})

export class RuleActionComponent implements OnInit, OnDestroy {

  @Input() datasets: string[];
  @Input() active = true;

  reSendAction?: RuleAction = null;
  result?: RuleActionResult;
  asyncID?: string;
  asyncResult: RuleResults;
  asyncAction = '';
  asyncSummary: {status: string, result: string} = {status: '', result: ''};

  summaryDatasetsResult: {status: string, result: string} = {status: '', result: ''};
  actionExecution$: Observable<{action: string, actions_result: string,
    datasets_result: {dataset: string, type: string, result: string}[]}>;
  actionExecuting = false;
  RULEACTIONS = {
    alter_source_replication_rule: {name: 'Alter source', params_name: ['cancel', 'change_source_rule', 'new_source']},
    change_destination: {name: 'Change destination', params_name: []},
    bypass_queue: {name: 'Bypass data carousel', params_name: []},


  };
   SINGLE_RULE_CONFIRMATION_REQUIRED = [ ];
   comment = '';


  constructor(private taskService: TaskService, public dialog: MatDialog) { }

  ngOnDestroy(): void {
    this.active = false;
  }

  ngOnInit(): void {
     this.actionExecution$ = this.taskService.getRuleActionList().pipe(
       filter(value => (this.datasets.length > 0) && (value !== null) && (this.active)),
       tap(_ => {
                         this.actionExecuting = true;
                         this.summaryDatasetsResult = {status: '', result: ''};
                         this.asyncSummary = {status: '', result: ''};
                         this.asyncResult = {action: '', actions_result: '', datasets_result: []};
                       }),
       switchMap((ruleAction) => {
         this.reSendAction = {action: ruleAction.action, datasets: [], comment: ruleAction.comment, params: ruleAction.params,
         action_name: this.RULEACTIONS[ruleAction.action].name, params_name: this.RULEACTIONS[ruleAction.action].params_name};
         return this.taskService.submitRuleAction(ruleAction.datasets, ruleAction.action,
           ruleAction.comment, ruleAction.params);
       }),
       tap(_ => this.actionExecuting = false),
       map(ruleActionResult => {
         if (ruleActionResult?.async_id) {
            this.asyncID = ruleActionResult.async_id;
            this.asyncAction = ruleActionResult.action;
          }
         if (ruleActionResult.datasets.length === 0){
            return {action: ruleActionResult.action, actions_result: 'async',
              datasets_result: []};
          }

         if (!ruleActionResult.action_sent && ruleActionResult.error && (ruleActionResult.error !== '')){
               return {action: ruleActionResult.action, actions_result: 'error',
                 datasets_result: [{datasets: ruleActionResult.datasets[0], type: 'error', result: ruleActionResult.error}]};
          }

         if (this.datasets.length === 1){
           if (!ruleActionResult.action_sent){
             if (ruleActionResult.action_verification.length === 1){
               if (ruleActionResult.action_verification[0].action_allowed &&
                 !ruleActionResult.action_verification[0].user_allowed){
                 return {action: ruleActionResult.action, actions_result: 'error',
                   datasets_result: [{dataset: ruleActionResult.action_verification[0].dataset, type: 'error', result: 'User permission is insufficient to execute the action'}]};
               }
               return {action: ruleActionResult.action, actions_result: 'error',
                  datasets_result: [{dataset: ruleActionResult.action_verification[0].dataset, type: 'error', result:  `The action is not allowed for a dataset`}]};
             }
           }
           if (ruleActionResult.result.length === 1) {
             if (ruleActionResult.result[0].return_info !== null && ruleActionResult.result[0].return_info.includes('Command rejected')){
               return {action: ruleActionResult.action, actions_result: 'sent',
                 datasets_result: [{dataset: ruleActionResult.result[0].dataset, type: 'warning', result:  `The command was sent to JEDI, return info:
              ${ruleActionResult.result[0].return_info}; return code: ${ruleActionResult.result[0].return_code};`}]};
             }
             return {action: ruleActionResult.action, actions_result: 'sent',
                 datasets_result: [{dataset: ruleActionResult.result[0].dataset, type: 'task_alt', result:  `The command was sent to JEDI, return info:
              ${ruleActionResult.result[0].return_info}; return code: ${ruleActionResult.result[0].return_code};`}]};

           }
         } else {
           const ruleResults = [];
           if (!ruleActionResult.action_sent) {
             const reSendDatasets = [];
             for (const actionVerification of ruleActionResult.action_verification) {
               if (actionVerification.action_allowed && !actionVerification.user_allowed) {
                 ruleResults.push({
                   dataset: actionVerification.dataset,
                   type: 'error',
                   result: 'User permission is insufficient to execute the action'
                 });
               }
               if (!actionVerification.action_allowed) {
                 ruleResults.push({
                   dataset: actionVerification.dataset,
                   type: 'error',
                   result: `The action is not allowed for a dataset`
                 });
               }
               if (actionVerification.action_allowed && actionVerification.user_allowed) {
                 ruleResults.push({
                   dataset: actionVerification.dataset,
                   type: 'task_alt',
                   result: `The action is allowed for a dataset`
                 });
                 reSendDatasets.push(actionVerification.dataset);
               }
               if (reSendDatasets.length > 0) {
                 this.reSendAction.datasets = this.datasets.filter(dataset => reSendDatasets.includes(dataset));
               }
             }
             let returnString = `Error: commands were not sent to JEDI. ${ruleResults.length - reSendDatasets.length} has problems`;
             if (reSendDatasets.length > 0){
                returnString += `, for ${reSendDatasets.length} rule command can be sent again.`;
              }
             this.summaryDatasetsResult = {status: 'error', result: returnString};
             return {action: ruleActionResult.action, actions_result: 'error', datasets_result: ruleResults};
           } else {
             const preparedResults =
               this.prepareResults(ruleActionResult.result, ruleActionResult.action, ruleResults, this.asyncID !== undefined);
             this.summaryDatasetsResult = preparedResults.summary;
             delete preparedResults.summary;
             return preparedResults;
           }

         }
       }
       ));
  }

  private prepareResults(actionResults: RulesResultInfo[], action: string,  ruleResults: any[], async = false):
    RuleResults & {summary: {status: string, result: string}} {
    let goodTasks = 0;
    let warningTasks = 0;
    let summary = {status: '', result: ''};
    for (const ruleResult of actionResults) {
      if (ruleResult.return_info !== null && (ruleResult.return_info.includes('Command rejected') || ruleResult.return_code.toString() === 'false')) {
        ruleResults.push({
          dataset: ruleResult.dataset,
          type: 'warning',
          result: `The command was sent to JEDI, return info: ${ruleResult.return_info}; return code: ${ruleResult.return_code};`
        });
        warningTasks++;
      } else {
        ruleResults.push({
          dataset: ruleResult.dataset,
          type: 'task_alt',
          result: `The command was sent to JEDI, return info: ${ruleResult.return_info}; return code: ${ruleResult.return_code};`
        });
        goodTasks++;
      }
    }
    if (warningTasks === 0) {
      if (!async){
        summary = {status: 'task_alt', result: `The commands were sent to JEDI`};
      } else {
        summary = {status: 'task_alt', result: `The commands are being submitted async`};
      }
    } else {
      summary = {
        status: 'warning',
        result: `The commands were sent to JEDI, ${warningTasks} datasets have problems`
      };
    }
    return {action, actions_result: 'sent', datasets_result: ruleResults, summary};
  }

  executeAction(action: string, params: ActionParams): void {
        if (  this.SINGLE_RULE_CONFIRMATION_REQUIRED.indexOf(action) > -1){
        this.dialog.open(DialogRuleSubmissionComponent, {data : {tasks: this.datasets, action, action_name: this.RULEACTIONS[action].name,
          params, params_name: this.RULEACTIONS[action].params_name, comment: this.comment}});
      } else {
        this.taskService.addRuleAction({datasets: this.datasets, action, action_name: this.RULEACTIONS[action].name,
            params, params_name: this.RULEACTIONS[action].params_name, comment: this.comment});
      }

  }
  stopPropagation(event: any): void{
    event.stopPropagation();
  }
  reSendRules(): void{
    this.taskService.addRuleAction(this.reSendAction);
  }


  asyncTaskFinished(asyncResult: AsyncProdTaskSplitStatus): void {
    this.asyncID = undefined;
    if (asyncResult.status === 'SUCCESS') {
      const asyncCurrentResult = this.prepareResults(asyncResult.result as RulesResultInfo[], this.asyncAction,  []);
      this.asyncSummary = asyncCurrentResult.summary;
      delete asyncCurrentResult.summary;
      this.asyncResult = asyncCurrentResult;
    } else {
      this.asyncSummary =  {status: 'error', result: this.result.toString()};
    }
  }
}

@Component({
  selector: 'app-dialog-rule-action',
  templateUrl: 'dialog-rule-action.html',
  imports: [
    MatButton,
    MatDialogActions,
    MatDialogContent,
    MatDialogTitle,
    MatFormField,
    MatInput,
    MatLabel,
    NgIf,
    ReactiveFormsModule,
    FormsModule
  ],
  standalone: true
})
export class DialogRuleSubmissionComponent implements OnInit{

  parameters = '';
  comment = '';

  constructor(@Inject(MAT_DIALOG_DATA) public data: RuleAction, public dialogRef: MatDialogRef<DialogRuleSubmissionComponent>,
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
    this.taskService.addRuleAction(this.data);
    this.dialogRef.close();
  }
}
