import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable, of, throwError} from "rxjs";
import {AnalysisSlice, TaskTemplate, TemplateBase} from "./analysis-task-model";
import {catchError} from "rxjs/operators";
import {PatternChanges} from "./pattern-edit/pattern-edit.component";
import {TaskActionResult} from "../production-task/task-service.service";

export interface AnalysisRequestActionResponse {
  result: string;
  error?: string;
}


@Injectable({
  providedIn: 'root'
})
export class AnalysisTasksService {

  private prTaskTemplateUrl = '/api/prepare_template_from_task';
  private prCreateTaskTemplateUrl = '/api/create_template/';
  private prGetTemplateUrl = '/api/get_template';
  private prGetAllTemplateUrl = '/api/get_all_templates';
  private prGetAnalysisRequestUrl = '/api/get_analysis_request';
  private prSaveTaskTemplateUrl = '/api/save_template_changes/';
  private prCreateAnalysisRequestUrl = '/api/create_analysis_request/';
  private prAnalysisRequestActionUrl = '/api/analysis_request_action/';
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

  getAllActiveTemplates(): Observable<TemplateBase[]> {
    return this.http.get<TemplateBase[]>(this.prGetAllTemplateUrl, {params: {active: 'true'}});
  }
  getAnalysisRequest(requestID: string): Observable<AnalysisSlice[]> {
    return this.http.get<AnalysisSlice[]>(this.prGetAnalysisRequestUrl, {params: {request_id: requestID}});
  }
  saveTemplateParams(templateID: string, templateBase: Partial<TemplateBase>|null,  params: PatternChanges): Observable<any> {
    return this.http.post(this.prSaveTaskTemplateUrl, {templateID, templateBase,  params});
  }

  createAnalysisRequest(description: string, templateBase: Partial<TemplateBase>, inputContainers: string[]): Observable<string> {
    return this.http.post<string>(this.prCreateAnalysisRequestUrl, {description, templateBase, inputContainers});
  }

  submitAnalysisRequestAction(requestID: string, action: string, slices: number[]): Observable<AnalysisRequestActionResponse> {
    return this.http.post<AnalysisRequestActionResponse>(this.prAnalysisRequestActionUrl, {requestID, action, slices}).pipe(
      catchError( err => {
         const result: AnalysisRequestActionResponse = {result: 'Error', error:  `Backend returned code ${err.status}, body was: ${err.error}`};
         return of(result);
      })
    );
  }
}
