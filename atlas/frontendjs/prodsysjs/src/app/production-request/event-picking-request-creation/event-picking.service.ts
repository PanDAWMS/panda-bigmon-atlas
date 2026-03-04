import {inject, Injectable, signal} from '@angular/core';
import {HttpClient, httpResource} from "@angular/common/http";
import {Observable} from "rxjs";

export interface EpRequestStats {
  requests: EpRequestSummary[];
  productions: EpProcessingSummary[];
}

export interface EpRequestSummary {
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
  private DeleteEPProcessURL = '/production_request/delete_ep_progress/';
  private GetEPRequestForUpdate = '/production_request/get_ep_request/';

  http = inject(HttpClient);
  constructor() { }

  EPProgressResource = httpResource<EpRequestStats>(
    () => this.jira() ? `${this.EPProgressURL}?jira=${this.jira()}` : '');

  jira = signal('');

  getEPRequestsList(): Observable<EPRequestShort[]>{
    return this.http.get<EPRequestShort[]>(this.EPRequestsURL);
  }

  submitEPRequest(jira: string): Observable<string>{
    return this.http.post<string>(this.SubmitEPrequestURL, {jira});
  }
  deleteEPProces(id: number): Observable<string>{
    return this.http.post<string>(this.DeleteEPProcessURL, {id});
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
