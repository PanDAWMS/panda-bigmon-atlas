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
  modification_time: number;
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
  staging_dataset?: StagingRule;
}
export interface DatasetDeletedResponse {
  dataset_name: string;
  error: string;
}

/**
 * Interface for the counts of different reason texts for a specific destination endpoint.
 * Example: { 'TRANSFER ERROR: Copy failed': 5, 'TRANSFER ERROR: No space left': 2 }
 */
export interface ReasonCounts {
  [reasonText: string]: number;
}

/**
 * Interface for the data associated with a specific destination endpoint.
 * Currently, it primarily holds reason counts.
 */
export interface DstEndpointData {
  reason_counts: ReasonCounts;
}

/**
 * Interface for the collection of destination endpoints under a source endpoint.
 * Example: { 'NET2_DATADISK': { reason_counts: {...} }, 'FZK_DATADISK': { reason_counts: {...} } }
 */
export interface DstEndpointsCollection {
  [dstEndpointName: string]: DstEndpointData;
}

/**
 * Interface for the data associated with a specific source endpoint.
 */
export interface SrcEndpointData {
  src_url: string;
  fts_link: string;
  fts_state: string;
  fts_submitted: string;
  first_attempt_start_time: number;
  last_attempt_end_time: number;
  dst_endpoints: DstEndpointsCollection;
}

/**
 * Interface for the collection of source endpoints under a file name.
 * Example: { 'TRIUMF-LCG2_MCTAPE': {...}, 'IN2P3-CC_DATADISK': {...} }
 */
export interface SrcEndpointsCollection {
  [srcEndpointName: string]: SrcEndpointData;
}

/**
 * The top-level interface for the aggregated transfer data.
 * Keys are file names.
 * Example: { 'file1.root': {...}, 'file2.root': {...} }
 */
export interface AggregatedTransferData {
  [fileName: string]: SrcEndpointsCollection;
}

@Injectable({
  providedIn: 'root'
})
export class DataCarouselService {

  constructor(private http: HttpClient) { }
  private prDataCarouselConfigUrl = '/api/data_carousel_config/';
  private prGetStagingRulesUrl = '/prestage/get_staging_rules/';
  private prGetDatasetInfoUrl = '/api/dataset_info/';
  private prGetStuckFilesUrl = '/api/get_stuck_files/';


  selectedTask = signal<string>('');
  datasetName = signal<string>('');
  selectedDCDataset = signal<string>('');
  stagingDataset = signal<string>('');
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

  stuckFilesResource = httpResource<AggregatedTransferData>(() => this.stagingDataset() ?
    `${this.prGetStuckFilesUrl}?dataset=${this.stagingDataset()}` : '');

  getDataCarouselConfig(): Observable<CarouselConfig> {
    return this.http.get<CarouselConfig>(this.prDataCarouselConfigUrl);
  }


}
