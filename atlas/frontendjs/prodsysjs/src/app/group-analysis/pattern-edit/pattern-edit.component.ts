import {Component, Input, OnChanges, OnInit, Output, SimpleChanges} from '@angular/core';
import {TaskTemplate} from "../analysis-task-model";
import {JsonFormControls, JsonFormData} from "../../dynamic-form-w-filter/dynamic-form-w-filter.component";
import {TASKS_CONSTANTS} from "../../common/constants/tasks_constants";
import {Subject} from "rxjs";
import {MatLegacySlideToggleChange as MatSlideToggleChange} from "@angular/material/legacy-slide-toggle";

export interface PatternChanges {
  changes: Partial<TaskTemplate>;
  removedFields: string[];
}

export type editState = 'edit' | 'view' | 'disabled';
@Component({
  selector: 'app-pattern-edit',
  templateUrl: './pattern-edit.component.html',
  styleUrls: ['./pattern-edit.component.css']
})
export class PatternEditComponent implements OnInit, OnChanges {
  @Input() pattern: Partial<TaskTemplate>;
  @Input() submitButtonText = 'Submit';
  @Input() editMode: editState = 'view';
  @Output() editModeChange = new Subject<editState>();
  @Output() patternChange = new Subject<PatternChanges>();
  public preparedTaskParamsForm: JsonFormData;
  public jobParameters: string[] = [];

  constructor() {
  }

  public KEYS_TO_HIDE = ['buildSpec', 'osInfo', 'vo', 'log', 'jobParameters', 'taskType', 'sourceURL', 'prodSourceLabel',
    'processingType', 'cliParams', 'dsForIN', 'mergeSpec', 'official', 'taskName', 'uniqueTaskName', 'userName'];
  public editModeToggle = false;

  ngOnChanges(changes: SimpleChanges): void {

    if (changes.pattern) {
      this.editMode = 'view';
      this.editModeChange.next(this.editMode);
      this.prepareForm();
    }
    this.editModeToggle = this.editMode === 'edit';
  }

  ngOnInit(): void {
    this.editModeToggle = this.editMode === 'edit';
    this.prepareForm();
  }

  private prepareForm(): void {
    this.preparedTaskParamsForm = {controls: []};
    const paramsTemplate = TASKS_CONSTANTS.TASKS_PARAMS_FORM.task_params_control;
    for (const key of Object.keys(this.pattern)) {
      if (this.KEYS_TO_HIDE.indexOf(key) === -1) {
        const param: Partial<JsonFormControls> = paramsTemplate.find((item) => item.name === key);
        if (param) {
          param.value = this.pattern[key];
          param.show = true;
          this.preparedTaskParamsForm.controls.push(param as JsonFormControls);
        }
      }
    }
    for (const paramTemplate of paramsTemplate) {
      if (this.KEYS_TO_HIDE.indexOf(paramTemplate.name) === -1) {
        const param: Partial<JsonFormControls> = paramTemplate;
        if (Object.keys(this.pattern).indexOf(paramTemplate.name) === -1) {
          param.value = null;
          param.show = false;
          this.preparedTaskParamsForm.controls.push(param as JsonFormControls);
        }
      }
    }
    this.jobParameters = [];
    for (const jobItem of this.pattern.jobParameters) {
      if ((jobItem?.hidden !== true) && (jobItem?.value !== null) && (jobItem?.value !== undefined)) {
        this.jobParameters.push(jobItem?.value);
      }
    }
  }

  onFormSubmit(data: JsonFormData): void {
    // Check if pattern changed
    this.editMode = 'view';
    this.editModeChange.next('view');
    const changes: Partial<TaskTemplate> = {};
    const formFields = data.controls;
    const removedFields: string[] = [];
    for (const formField of formFields) {
      if (Object.keys(this.pattern).indexOf(formField.name) === -1) {
        if (formField.show)  {
          changes[formField.name] = formField.value;
        }
      } else {
        if (!formField.show) {
          removedFields.push(formField.name);
        } else {
          if (formField.value !== this.pattern[formField.name]) {
            changes[formField.name] = formField.value;
          }
        }
      }
    }
    this.patternChange.next({changes,  removedFields});
  }

  onFormReset(data: JsonFormData): void {
      this.preparedTaskParamsForm = data;
      this.editMode = 'view';
      this.editModeChange.next('view');
  }

  toggleEditMode($event: MatSlideToggleChange): void {
    if ($event.checked) {
        this.editMode = 'edit';
        this.editModeChange.next('edit');
    } else {
      this.editMode = 'view';
      this.editModeChange.next('view');
    }
  }
}
