import {Component, inject, Input, signal} from '@angular/core';
import {ProdsysJsoneditorComponent} from '../common/prodsys-jsoneditor/prodsys-jsoneditor.component';
import {JSONEditorOptions} from 'jsoneditor';
import {JsondiffComponent} from '../common/jsondiff/jsondiff.component';
import {JsonPipe} from '@angular/common';
import {JsonGdpconfigEditorService} from "./json-gdpconfig-editor.service";
import {MatProgressSpinner} from "@angular/material/progress-spinner";
import {toObservable} from "@angular/core/rxjs-interop";
import {
  MatAccordion,
  MatExpansionPanel,
MatExpansionPanelHeader,
  MatExpansionPanelTitle
} from "@angular/material/expansion";
import {MatButton} from "@angular/material/button";




@Component({
  selector: 'app-json-gdpconfig-editor',
  standalone: true,
  imports: [
    ProdsysJsoneditorComponent,
    JsondiffComponent,
    JsonPipe,
    MatProgressSpinner,
    MatAccordion,
    MatExpansionPanel,
    MatExpansionPanelTitle,
    MatExpansionPanelHeader,
    MatButton,
  ],
  templateUrl: './json-gdpconfig-editor.component.html',
  styleUrl: './json-gdpconfig-editor.component.css'
})
export class JsonGDPConfigEditorComponent{

  @Input() set parameter(value: string){
    this.parameter$.set(value);
    this.gdpConfigService.setKey(value);
  }

  gdpConfigService = inject(JsonGdpconfigEditorService);

  data = this.gdpConfigService.value;
  isLoading = this.gdpConfigService.isLoading;
  errorMessage = this.gdpConfigService.errorMessage;
  saved = this.gdpConfigService.saved;
  preparedData = toObservable(this.data);
  parameter$ = signal('');


  originalData: any = {};
  workingData: any = {};
  schema: any  = {};

  haveMetadata = false;
    mode: 'edit'|'preview' = 'edit';
    errorState = false;
    editorOptions: JSONEditorOptions = {
        mode: 'code',
        modes: ['code', 'form', 'tree', 'view'],
    };

  constructor() {
    this.preparedData.subscribe((data) => {
      if (data === null || data === undefined) {
        return;
      }
      if ('data' in data && 'metadata' in data) {
        this.originalData = {...data.data};
        this.workingData = {...data.data};
        this.haveMetadata = true;
      } else {
        this.originalData = {...data};
        this.workingData = {...data};
        this.haveMetadata = false;
      }
      if (this.haveMetadata && 'schema' in data.metadata) {
        this.schema = data.metadata.schema;
      }
    });
  }

  save(): void {
    if (this.haveMetadata) {
      this.gdpConfigService.saveKey(this.parameter$(), {metadata: this.data().metadata, data: this.workingData});
    } else {
      this.gdpConfigService.saveKey(this.parameter$(), this.workingData);
    }
    this.mode = 'edit';
  }
  protected readonly JSON = JSON;
}
