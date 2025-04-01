import {computed, Injectable} from '@angular/core';
import {HttpClient, httpResource} from "@angular/common/http";
import {Observable} from "rxjs";
import {SelectionChangedEvent} from "ag-grid-community";

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
  tasks_ids: number[];
}
export interface StagingRuleResponse {
  rules: StagingRule[];
  fullRSEs: string[];
}
@Injectable({
  providedIn: 'root'
})
export class DataCarouselService {

  constructor(private http: HttpClient) { }
  private prDataCarouselConfigUrl = '/api/data_carousel_config/';
  private prGetStagingRulesUrl = '/prestage/get_staging_rules/';
  datasetStagingRulesResource = httpResource<StagingRuleResponse>(this.prGetStagingRulesUrl);


  getDataCarouselConfig(): Observable<CarouselConfig> {
    return this.http.get<CarouselConfig>(this.prDataCarouselConfigUrl);
  }


}
