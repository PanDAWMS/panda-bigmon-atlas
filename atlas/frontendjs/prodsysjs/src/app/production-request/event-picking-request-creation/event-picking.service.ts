import {inject, Injectable, signal} from '@angular/core';
import {HttpClient, httpResource} from "@angular/common/http";
import {Observable} from "rxjs";

export interface EpRequestStats {
  requests: EpRequestSummary[];
  productions: EpProcessingSummary[];
}

export interface EpRequestSummary {
  id: number;
  jira: string;
  stream: string;
  description: string;
  requestor: string;
  unique_files: number;
  unique_events: number;
}

export interface EpProcessingSummary {
  id: number;
  status: string;
  stream: string;
  project: string;
  logs: string;
  stats: EpProcessingStats;
  production_request_id: number | null;
}
export interface ProducedDataset {
  name: string;
  events: number | null;
  status: string | null;
  version: string;
  project: string;
}

export interface ProducedDatasetsResponse {
  datasets: ProducedDataset[];
  containers: string[];
}
export interface EpProcessingStats {
  running_tasks?: number;
  finished_tasks?: number;
  done_tasks?: number;
  picked?: {
    runs: number,
    events: number,
    files: number
  };
  produced?: {
    runs: number,
    events: number,
  };
  [key: string]: any; // For other stats fields
}
export interface EPRequestShort{
  jira: string;
  requestor: string;
  description: string;
}
@Injectable({
  providedIn: 'root'
})
export class EventPickingService {
  private createOrUpdateEPRequestURL = '/api/create_ep_request/';
  private EPRequestsURL = '/api/ep_requests/';
  private EPProgressURL = '/production_request/ep_request_stats/';
  private SubmitEPrequestURL = '/production_request/submit_ep_request/';
  private RetryEPProcessURL = '/production_request/retry_ep_progress/';
  private GetEPRequestForUpdate = '/production_request/get_ep_request/';
  private GetEPEventsURL = '/production_request/get_ep_events/';
  private GetResultEPURL = '/production_request/get_ep_result_datasets/';
  private RegisterEPContainerURL = '/production_request/register_ep_container/';

  http = inject(HttpClient);
  constructor() { }

  EPProgressResource = httpResource<EpRequestStats>(
    () => this.jira() ? `${this.EPProgressURL}?jira=${this.jira()}` : '');

  jira = signal('');

  registerEPContainer(jira: string, container: string, datasets: string[]): Observable<string> {
    return this.http.post<string>(this.RegisterEPContainerURL, {
      jira,
      container,
      datasets
    });
  }

  getEPRequestsList(): Observable<EPRequestShort[]>{
    return this.http.get<EPRequestShort[]>(this.EPRequestsURL);
  }

  getEPEvents(eventsType: string, id: string): Observable<any>{
    return this.http.get<any>(this.GetEPEventsURL, {params: {type: eventsType, id}});
  }

  getProducedDatasets(jira: string): Observable<ProducedDatasetsResponse> {
    return this.http.post<ProducedDatasetsResponse>(this.GetResultEPURL, { jira });
  }

  submitEPRequest(jira: string): Observable<string>{
    return this.http.post<string>(this.SubmitEPrequestURL, {jira});
  }
  retryEPProces(id: number): Observable<string>{
    return this.http.post<string>(this.RetryEPProcessURL, {id});
  }
  getEPRequestForUpdate(jira: string): Observable<{description: string, merge: boolean}|null>{
    return this.http.post<{description: string, merge: boolean}|null>(this.GetEPRequestForUpdate, {jira});
  }
  createOrUpdateEPRequest(jira: string, stream: string, description: string,
                          dataFormat: string, merge: boolean, submit: boolean, content: string): Observable<string>{
    return this.http.post<string>(this.createOrUpdateEPRequestURL, {
      jira,
      stream,
      description,
      data_format: dataFormat,
      content,
      merge,
      submit
    });
  }


}
