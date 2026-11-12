import {inject, Injectable} from '@angular/core';
import {HttpClient, HttpParams} from '@angular/common/http';

export interface SearchFilters {
  q: string;
  step_name: string;
  ctag: string;
  pr_id_min: string;
  pr_id_max: string;
  task_id_min: string;
  task_id_max: string;
  run_number_min: string;
  run_number_max: string;
}

export interface SearchState extends SearchFilters {
  page: number;
  page_size: number;
  sort: string;
  direction: string;
}

export interface DkbTask {
  taskid: number;
  taskname: string;
  pr_id: number;
  status: string;
  campaign: string;
  subcampaign: string;
  step_name: string;
  ctag: string;
  project: string;
  total_events: number;
  processed_events: number;
  requested_events: number;
  input_events: number;
  run_number: number;
}

export interface DkbTaskDetails extends DkbTask {
  [key: string]: any;
  primary_input?: string | string[];
  output_dataset?: {name: string; bytes: number; events: number; cross_section: number; gen_filt_eff: number}[];
}

export interface TaskPage {
  tasks: DkbTask[];
  total: number;
  available: number;
  limit: number;
  page: number;
  page_size: number;
}

export interface SubcampaignSummary {
  subcampaigns: {subcampaign: string | null; tasks: number; total_events: number}[];
  total_tasks: number;
  total_events: number;
}

export const EMPTY_FILTERS: SearchFilters = {
  q: '', step_name: '', ctag: '', pr_id_min: '', pr_id_max: '', task_id_min: '', task_id_max: '',
  run_number_min: '', run_number_max: '',
};

@Injectable({providedIn: 'root'})
export class DkbSearchService {
  private http = inject(HttpClient);

  tasks(state: SearchState) {
    return this.http.get<TaskPage>('/dkb/opensearch/tasks/', {params: this.params(state)});
  }

  task(taskID: number) {
    return this.http.get<DkbTaskDetails>(`/dkb/opensearch/tasks/${taskID}/`);
  }

  summary(filters: SearchFilters) {
    return this.http.get<SubcampaignSummary>('/dkb/opensearch/summary/', {params: this.params(filters)});
  }

  private params(values: SearchFilters | SearchState): HttpParams {
    let params = new HttpParams();
    Object.entries(values).forEach(([key, value]) => params = params.set(key, String(value)));
    return params;
  }
}
