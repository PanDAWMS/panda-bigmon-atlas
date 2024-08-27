import {inject, Injectable, signal} from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {EMPTY, Observable} from "rxjs";
import {catchError} from "rxjs/operators";
import {setErrorMessage} from "../dsid-info/dsid-info.service";

export interface StageProfileSpans {
  x: string;
  y: number[];
  fillColor: string;
}

export interface TaskStageProfile {
  spans: StageProfileSpans[];
  done_attempts: number[];
  failed_attempts_after_success: number;
  done_attempts_after_success: number;
  files_staged: number;
  task: number;
  dataset: string;
  source: string;
  date_since: string;
  date_until: string;
  total_files: number;
  total_attempts: number;
}


@Injectable({
  providedIn: 'root'
})
export class TaskStageProfileService {
  private prPostPatchUrl = '/api/stage_profile';
  private http = inject(HttpClient);
  public error$ = signal('');
  public getTaskStageProfile(taskId: string, dataset: string = '', source: string = ''): Observable<TaskStageProfile> {
    return this.http.get<TaskStageProfile>(`${this.prPostPatchUrl}/${taskId}`, {params: {dataset, source}}).pipe(
      catchError((error: any) => {
        this.error$.set(setErrorMessage(error));
        return EMPTY;
      })
    );
  }

  constructor() { }
}
