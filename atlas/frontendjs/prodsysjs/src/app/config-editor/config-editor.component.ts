import {Component, computed, inject, Input} from '@angular/core';
import {ProdsysConfigService} from "./prodsys-config.service";
import {MatProgressSpinner} from "@angular/material/progress-spinner";
import {JSONEditorOptions} from "jsoneditor";
import {ProdsysJsoneditorComponent} from "../common/prodsys-jsoneditor/prodsys-jsoneditor.component";
import {toObservable} from "@angular/core/rxjs-interop";
import {MatButton} from "@angular/material/button";
import {JsondiffComponent} from "../common/jsondiff/jsondiff.component";

@Component({
  selector: 'app-config-editor',
  imports: [
    MatProgressSpinner,
    ProdsysJsoneditorComponent,
    MatButton,
    JsondiffComponent
  ],
  templateUrl: './config-editor.component.html',
  styleUrl: './config-editor.component.css'
})
export class ConfigEditorComponent {

  prodsysConfigService = inject(ProdsysConfigService);
  errorState = false;
  saving = false;
  editorOptions: JSONEditorOptions = {
        mode: 'code',
        modes: ['code', 'form', 'tree', 'view'],
    };

  @Input()
  set name(paramName: string){
      if (!paramName){
          return;
      }
      this.prodsysConfigService.parameterName.set(paramName);
  }
  mode: 'edit'|'preview' = 'edit';
  parameter$ = toObservable(this.prodsysConfigService.configParameter);
  errorMessage = computed( () => this.prodsysConfigService.errorMessage() + this.prodsysConfigService.saveErrorMessage());
  isLoading = this.prodsysConfigService.isLoading;
  value = {};
  schema = {};
  protected readonly JSON = JSON;

  constructor() {
    this.parameter$.subscribe(parameter => {
      if (parameter){
        this.value = parameter.value;
        this.schema = parameter.schema;
      }
    });
  }

  save(): void {
    this.saving = true;
    this.prodsysConfigService.saveConfigParameter(this.value).subscribe(() => {this.saving = false; this.mode = 'edit'; });
  }



}
