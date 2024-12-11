import {Component, computed, inject, Input, Pipe, signal} from '@angular/core';
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
  MatExpansionPanelDescription, MatExpansionPanelHeader,
  MatExpansionPanelTitle
} from "@angular/material/expansion";
import {TaskStatsComponent} from "../production-request/task-stats/task-stats.component";
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
    MatExpansionPanelDescription,
    MatExpansionPanelHeader,
    TaskStatsComponent,
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
  preparedData = toObservable(this.data);
  parameter$ = signal('');


  originalData: any = {};
  workingData: any = {};
  schema: any  = {};
  test_schema = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "source_tapes_config": {
      "type": "object",
      "patternProperties": {
        "^[A-Za-z0-9-_]+$": {
          "type": "object",
          "properties": {
            "active": {
              "type": "boolean"
            },
            "max_size": {
              "type": "integer",
              "minimum": 0
            },
            "max_staging_ratio": {
              "type": "integer",
              "minimum": 0,
              "maximum": 100
            },
            "destination_expression": {
              "type": "string"
            }
          },
          "required": ["active", "max_size", "max_staging_ratio", "destination_expression"],
          "additionalProperties": false
        }
      }
    },
    "excluded_destinations": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "early_access_users": {
      "type": "array",
      "items": {
        "type": "string"
      }
    }
  },
  "required": ["source_tapes_config", "excluded_destinations", "early_access_users"],
  "additionalProperties": false
};
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
      if (this.parameter$() === 'DATA_CAROUSEL_CONFIGS'){
        this.schema = this.test_schema;
      }
    });
  }
  protected readonly JSON = JSON;
}
