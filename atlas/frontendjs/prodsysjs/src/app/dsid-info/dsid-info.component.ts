import {Component, inject, Input} from '@angular/core';
import {DSIDInfoService} from "./dsid-info.service";
import {MatProgressSpinner} from "@angular/material/progress-spinner";
import {DecimalPipe, JsonPipe} from "@angular/common";
import {objectKeys} from "codelyzer/util/objectKeys";
import {MatButton} from "@angular/material/button";
import {MatFormField, MatLabel} from "@angular/material/form-field";
import {MatInput} from "@angular/material/input";
import {FormsModule, ReactiveFormsModule} from "@angular/forms";
import {RouterLink} from "@angular/router";
import {RucioURLPipe} from "../derivation-exclusion/rucio-url.pipe";

@Component({
  selector: 'app-dsid-info',
  standalone: true,
  imports: [
    MatProgressSpinner,
    JsonPipe,
    DecimalPipe,
    MatButton,
    MatFormField,
    MatInput,
    MatLabel,
    ReactiveFormsModule,
    FormsModule,
    RouterLink,
    RucioURLPipe
  ],
  templateUrl: './dsid-info.component.html',
  styleUrl: './dsid-info.component.css'
})
export class DsidInfoComponent {

  dsidService = inject(DSIDInfoService);
  @Input() set dsid(value: number) {
    this.formDSID = value;
    this.dsidService.setSelectedDSID(value);
  }

  dsidInfo = this.dsidService.dsidInfo;
  isLoading = this.dsidService.isLoading;
  errorMessage = this.dsidService.errorMessage;
  constructor() { }

  protected readonly objectKeys = objectKeys;
  formDSID: number;

  getTasksUrl(): string {
    return `(taskname:${this.formDSID})`;
  }
}
