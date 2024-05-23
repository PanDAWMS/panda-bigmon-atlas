import {Component, OnInit} from '@angular/core';
import {FormBuilder, FormGroup, ReactiveFormsModule, Validators} from "@angular/forms";
import {MatStepperModule} from "@angular/material/stepper";
import {MatFormFieldModule} from "@angular/material/form-field";
import {MatInputModule} from "@angular/material/input";
import {MatButtonModule} from "@angular/material/button";
import {MatSelectModule} from "@angular/material/select";
import {PRODSYS_CONSTANTS} from "../../common/constants/tasks_constants";

@Component({
  selector: 'app-mc-request-submission',
  standalone: true,
  imports: [
    MatStepperModule,
    ReactiveFormsModule,
    MatFormFieldModule,
    MatInputModule,
    MatButtonModule,
    MatSelectModule
  ],
  templateUrl: './mc-request-submission.component.html',
  styleUrl: './mc-request-submission.component.css'
})
export class McRequestSubmissionComponent implements OnInit {
  isLinear = false;
  firstStepFormGroup: FormGroup;
  secondFormGroup: FormGroup;

  constructor(private _form_builder: FormBuilder) {}

  ngOnInit() {
    this.firstStepFormGroup = this._form_builder.group({
      descriptionCtrl: ['', Validators.required],
      refLinkCtrl: ['', Validators.required],
      physGroupCtrl: ['', Validators.required],
      longDescriptionCtrl: ['', Validators.required],
      spreadsheetCtrl: ['', Validators.required],
    });
    this.secondFormGroup = this._form_builder.group({
      energyCtrl: ['', Validators.required],
      ccCtrl: [''],
    });
  }

  multiplyAndCeil(numbers: number[], ratio: number): number[] {
    return numbers.map(num => {
      const result = num * ratio;
      if (result > 10000) {
        return Math.ceil(result / 10000) * 10000;
      } else if (result > 1000) {
        return Math.ceil(result / 1000) * 1000;
      } else {
        return Math.ceil(result);
      }
    });
  }
  protected readonly PRODSYS_CONSTANTS = PRODSYS_CONSTANTS;
}
