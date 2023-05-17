import {
  ChangeDetectionStrategy,
  Component,
  Input,
  OnChanges,
  OnInit,
  Output,
  Renderer2,
  SimpleChanges
} from '@angular/core';
import {FormBuilder, FormGroup, Validators} from "@angular/forms";
import {BehaviorSubject, Subject} from "rxjs";
import {debounceTime, distinctUntilChanged, map, switchMap, tap} from "rxjs/operators";

interface JsonFormControlOptions {
  min?: string;
  max?: string;
  step?: string;
  icon?: string;
}

interface JsonFormValidators {
  min?: number;
  max?: number;
  required?: boolean;
  requiredTrue?: boolean;
  minLength?: boolean;
  maxLength?: boolean;
  pattern?: string;
  nullValidator?: boolean;
}
export interface JsonFormControls {
  name: string;
  label: string;
  description: string;
  value: string|boolean|number;
  show: boolean;
  type: string;
  options?: JsonFormControlOptions;
  required: boolean;
  validators: JsonFormValidators;
}

export interface JsonFormData {
  controls: JsonFormControls[];
}
@Component({
  selector: 'app-dynamic-form-w-filter',
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './dynamic-form-w-filter.component.html',
  styleUrls: ['./dynamic-form-w-filter.component.css']
})
export class DynamicFormWFilterComponent implements OnChanges {

  @Input() jsonFormData: JsonFormData;
  @Input() submitButtonText = 'Submit';
  @Output() formSubmit = new Subject<JsonFormData>();
  @Output() formReset = new Subject<JsonFormData>();

  public mainForm: FormGroup = this.fb.group({parameterNameFilter: [''],
    parameterValueFilter: ['']});
  unchangedJsonFormData: JsonFormData;
  showAll = false;
  searchTerms$ = new BehaviorSubject<string>('');
  filteredControls$ = this.searchTerms$.pipe(debounceTime(300),
    tap( searchTerm => this.showAll = this.mainForm.get('parameterNameFilter').value.length !== 0),
    map((_) => {
      if (this.mainForm.get('parameterNameFilter').value.length !== 0) {
        return this.jsonFormData.controls.filter((control) => {
          return control.name.toLowerCase().includes(this.mainForm.get('parameterNameFilter').value.toString().toLowerCase());
        });
      }
      return this.jsonFormData.controls;
      }),
     map((controls) => {
       if (this.mainForm.get('parameterValueFilter').value.length !== 0) {
         return controls.filter((control) => {
           if (control.value === null) {
             return false;
           }
           return control.value.toString().toLowerCase().includes(this.mainForm.get('parameterValueFilter').value.toString().toLowerCase());
         });
       }
       return controls;
      }));
  numberHiddenParams: number;

  constructor(private fb: FormBuilder, private renderer: Renderer2) { }

  ngOnChanges(changes: SimpleChanges): void {
    this.unchangedJsonFormData = {controls: []};
    for (const control of this.jsonFormData.controls) {
      this.unchangedJsonFormData.controls.push({...control});
    }
    this.createForm(this.jsonFormData.controls);
    this.numberHiddenParams = this.jsonFormData.controls.filter((control) => !control.show).length;
  }
  createForm(controls: JsonFormControls[]): void {
    const taskParamForm = this.fb.group({});
    for (const control of controls) {
      const validatorsToAdd = [];
      for (const [key, value] of Object.entries(control.validators)) {
        switch (key) {
          case 'min':
            validatorsToAdd.push(Validators.min(value));
            break;
          case 'max':
            validatorsToAdd.push(Validators.max(value));
            break;
          case 'required':
            if (value) {
              validatorsToAdd.push(Validators.required);
            }
            break;
          case 'requiredTrue':
            if (value) {
              validatorsToAdd.push(Validators.requiredTrue);
            }
            break;
          case 'email':
            if (value) {
              validatorsToAdd.push(Validators.email);
            }
            break;
          case 'minLength':
            validatorsToAdd.push(Validators.minLength(value));
            break;
          case 'maxLength':
            validatorsToAdd.push(Validators.maxLength(value));
            break;
          case 'pattern':
            validatorsToAdd.push(Validators.pattern(value));
            break;
          case 'nullValidator':
            if (value) {
              validatorsToAdd.push(Validators.nullValidator);
            }
            break;
          default:
            break;
        }
      }
      taskParamForm.addControl(
        control.name,
        this.fb.control(control.value, validatorsToAdd)
      );
      this.mainForm.addControl('taskParams', taskParamForm);
    }
  }
  onCancel(): void {
    this.mainForm.get('taskParams').reset();
    this.jsonFormData = {controls: []};
    for (const control of this.unchangedJsonFormData.controls) {
      this.jsonFormData.controls.push({...control});
    }
    this.mainForm.get('parameterNameFilter').setValue('');
    this.mainForm.get('parameterValueFilter').setValue('');
    this.searchTerms$.next('');
    this.showAll = false;
    this.formReset.next(this.jsonFormData);
  }
  onSubmit(): void {
    this.jsonFormData.controls.forEach((control) => { control.value = this.mainForm.get('taskParams').get(control.name).value; });
    this.formSubmit.next(this.jsonFormData);
  }

  toggleParams(control: JsonFormControls): void {
    if (control.type === 'boolean') {
      this.mainForm.get('taskParams').get(control.name).setValue(true);
    }
    control.show = !control.show;
    this.numberHiddenParams = this.jsonFormData.controls.filter((x) => !x.show).length;
    this.mainForm.get('parameterNameFilter').setValue('');
    this.searchTerms$.next('');
    this.showAll = false;
    if (control.show && (control.type !== 'boolean')) {
      setTimeout(() => {
        this.renderer.selectRootElement(`#${control.name}`).focus();
      }, 200);
    }
  }
}
