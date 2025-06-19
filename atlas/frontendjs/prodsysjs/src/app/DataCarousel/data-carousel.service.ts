import {computed, Injectable, signal} from '@angular/core';
import {HttpClient, httpResource} from "@angular/common/http";
import {Observable} from "rxjs";
import {SelectionChangedEvent} from "ag-grid-community";
import {DatasetInfo, RucioReplica, RucioRule} from "../production-request/production-request-models";

export interface CarouselTapeConfig {
  tapeName: string;
  baseRule: string;
  active: boolean;
  min_bulksize: number;
  max_bulksize: number;
  batchdelay: number;
}

export interface CarouselConfig {
  carouselTapes: CarouselTapeConfig[];
  excludeSites: string[];
}


export interface StagingRule {
  id: number;
  dataset: string;
  scope: string;
  data_type: string;
  status: string;
  rse: string;
  source: string;
  destination: string;
  total_files: number;
  staged_files: number;
  start_time: number;
  update_time: number;
  number_active_tasks: number;
  dc_type: string;
  owners: string[];
  stuck: boolean;
  stuck_error: string;
  empty_source: boolean;
  tasks_ids: number[];
}
export interface StagingRuleResponse {
  rules: StagingRule[];
  fullRSEs: string[];
}
export interface DatasetInfoResponse {
  dataset_exists: boolean;
  dataset_knowledge: DatasetExistsResponse|DatasetDeletedResponse;
}
export interface DatasetExistsResponse {
  dataset: DatasetInfo;
  replicas: RucioReplica[];
  rules: RucioRule[];
  staging_dataset?: string;
}
export interface DatasetDeletedResponse {
  dataset_name: string;
  error: string;
}
@Injectable({
  providedIn: 'root'
})
export class DataCarouselService {

  constructor(private http: HttpClient) { }
  private prDataCarouselConfigUrl = '/api/data_carousel_config/';
  private prGetStagingRulesUrl = '/prestage/get_staging_rules/';
  private prGetDatasetInfoUrl = '/api/dataset_info/';


  selectedTask = signal<string>('');
  datasetName = signal<string>('');
  selectedDCDataset = signal<string>('');
  getAllRules = signal<boolean>(false);
  datasetStagingRulesResource = httpResource<StagingRuleResponse>(() => {
        if (this.selectedDCDataset()) {
          return `${this.prGetStagingRulesUrl}?dc=${this.selectedDCDataset()}`;
        } else if (this.selectedTask()) {
          return `${this.prGetStagingRulesUrl}?task_id=${this.selectedTask()}`;
        } else if (this.getAllRules()) {
          return `${this.prGetStagingRulesUrl}`;
        }
        return '';
      }
    );

  datasetInfoResource = httpResource<DatasetInfoResponse>(() => this.datasetName() ?
    `${this.prGetDatasetInfoUrl}?dataset=${this.datasetName()}` : '');


  getDataCarouselConfig(): Observable<CarouselConfig> {
    return this.http.get<CarouselConfig>(this.prDataCarouselConfigUrl);
  }


}
