import {AfterViewInit, Component, OnInit, ViewChild} from '@angular/core';
import {FormBuilder, Validators} from "@angular/forms";
import {MatStepper} from "@angular/material/stepper";
import {Observable, throwError} from "rxjs";
import {AnalysisTasksService} from "../analysis-tasks.service";
import {catchError, switchMap, tap} from "rxjs/operators";
import {ANALISYS_SOURCE_ACTIONS, TaskTemplate} from "../analysis-task-model";
import {ActivatedRoute, Router} from "@angular/router";
import {editState, PatternChanges} from "../pattern-edit/pattern-edit.component";

@Component({
  selector: 'app-task-template-submission',
  templateUrl: './task-template-submission.component.html',
  styleUrls: ['./task-template-submission.component.css']
})
export class TaskTemplateSubmissionComponent implements OnInit, AfterViewInit {
   @ViewChild('taskTemplateStepper') stepper: MatStepper;

  taskIDFormGroup = this.formBuilder.group({
    taskIDCtrl: ['', Validators.required],
  });
  templateDescriptionFormGroup = this.formBuilder.group({
    templateDescriptionCtrl: ['', Validators.required],
    templateSourceActionCtrl: [''],
  });
  public taskTemplate$: Observable<Partial<TaskTemplate>>;
  public currentTaskTemplate: TaskTemplate;
  public submissionError: string;
  public editMode: editState = 'view';
  constructor(private formBuilder: FormBuilder, private analysisTaskService: AnalysisTasksService, private router: Router,
              private route: ActivatedRoute) { }

  ngOnInit(): void {
     this.route.queryParamMap.subscribe((queryParams) => {
        if (queryParams.has('taskID')){
          this.taskIDFormGroup.get('taskIDCtrl').setValue( queryParams.get('taskID'));
        }
     }
      );

  }
       ngAfterViewInit(): void {
               this.taskTemplate$ = this.stepper.selectionChange.pipe(
        switchMap((event) => {
          if (event.selectedIndex === 1) {
            return this.analysisTaskService.getTaskTemplate(this.taskIDFormGroup.get('taskIDCtrl').value);
          }
        }), tap((taskTemplate) => { this.currentTaskTemplate = taskTemplate as TaskTemplate; }));
    }

  createTemplate(): void {
    this.analysisTaskService.createTaskTemplate(this.currentTaskTemplate,
      this.taskIDFormGroup.get('taskIDCtrl').value,
      this.templateDescriptionFormGroup.get('templateDescriptionCtrl').value,
      this.templateDescriptionFormGroup.get('templateSourceActionCtrl').value).pipe(
      catchError( err =>  this.submissionError = err.error)).
    subscribe((taskTemplateID) => { this.router.navigate(['analysis-pattern', taskTemplateID]); });
  }
  changeTemplate(data: PatternChanges): void {
    for (const key of data.removedFields){
      delete this.currentTaskTemplate[key];
    }
    for (const key of Object.keys(data.changes)){
      this.currentTaskTemplate[key] = data.changes[key];
    }
  }

  protected readonly ANALISYS_SOURCE_ACTIONS = ANALISYS_SOURCE_ACTIONS;
}
