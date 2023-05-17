import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable, throwError} from "rxjs";
import {AnalysisSlice, TaskTemplate, TemplateBase} from "./analysis-task-model";
import {catchError} from "rxjs/operators";
import {PatternChanges} from "./pattern-edit/pattern-edit.component";



@Injectable({
  providedIn: 'root'
})
export class AnalysisTasksService {

  private prTaskTemplateUrl = '/api/prepare_template_from_task';
  private prCreateTaskTemplateUrl = '/api/create_template/';
  private prGetTemplateUrl = '/api/get_template';
  private prGetAnalysisRequestUrl = '/api/get_analysis_request';
  private prSaveTaskTemplateUrl = '/api/save_template_changes/';
  constructor(private http: HttpClient) { }

  getTaskTemplate(taskID: string): Observable<Partial<TaskTemplate>> {
    return this.http.get<Partial<TaskTemplate>>(this.prTaskTemplateUrl, {params: {task_id: taskID }});
  }

  createTaskTemplate(taskTemplate: TaskTemplate, taskID: string, description: string): Observable<string> {
    return this.http.post<string>(this.prCreateTaskTemplateUrl, {taskTemplate, taskID, description}).pipe(
      catchError( err => {
        if (err.status !== '500') {
          return throwError( `Error template creation ${err.error}`);
        } else {
          return throwError( `Error template creation ${err.error} (status ${err.status})`);
        }
      }));
  }

  getTemplate(templateID: string): Observable<TemplateBase> {
    return this.http.get<TemplateBase>(this.prGetTemplateUrl, {params: {template_tag: templateID}});
  }

  getAnalysisRequest(requestID: string): Observable<AnalysisSlice[]> {
    return this.http.get<AnalysisSlice[]>(this.prGetAnalysisRequestUrl, {params: {request_id: requestID}});
  }
  saveTemplateParams(templateID: string, templateBase: Partial<TemplateBase>|null,  params: PatternChanges): Observable<any> {
    return this.http.post(this.prSaveTaskTemplateUrl, {templateID, templateBase,  params});
  }

}
