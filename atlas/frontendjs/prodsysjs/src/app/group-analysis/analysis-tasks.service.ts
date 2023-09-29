import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable, of, throwError} from "rxjs";
import {AnalysisSlice, TaskTemplate, TemplateBase} from "./analysis-task-model";
import {catchError} from "rxjs/operators";
import {PatternChanges} from "./pattern-edit/pattern-edit.component";
import {DerivationContainersCollection} from "../derivation-from-tag/derivation-request-models";

export interface AnalysisRequestActionResponse {
  result: string;
  error?: string;
}

export interface AnalysisModificationActionResponse {
  slicesToModify: number[];
  template: Partial<TaskTemplate>|null;
  error?: string;
}

export interface AnalysisRequestStats {
  hs06sec_finished: number;
  hs06sec_failed: number;
  bytes: number;
}

export interface ParentDerivationRequest {
  slices: {slice: number, outputFormat: string, container: string}[];
  outputFormats: string[];
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
  private prGetAnalysisRequestStatsUrl = '/api/analysis_request_stats/';
  private prGetAnalysisRequestOutputsUrl = '/api/get_analysis_request_output_datasets_names/';
  private prGetTaskPreviewUrl = '/api/get_analysis_task_preview/';
  private prGetPatternViewUrl = '/api/get_analysis_pattern_view/';
  private prGetParentDerivationUrl = '/api/get_derivation_slices/';
  private prGetAnalysisRequestHashtagsUrl = '/api/get_analysis_request_hashtags/';
  private prAddAnalysisRequestHashtagUrl = '/api/add_delete_analysis_request_hashtag/';



  constructor(private http: HttpClient) { }

   getAnalysisTaskPreview(requestID: string, sliceNumber: string): Observable<string> {
    return this.http.get<string>(this.prGetTaskPreviewUrl, {params: {requestID, sliceNumber}}).pipe(
      catchError( err => {
        if (err.status !== '500') {
          return throwError( `Error task preview ${err.error}`);
        } else {
          return throwError( `Error task preview ${err.error} (status ${err.status})`);
        }
      }));
  }
  getAnalysisPatternView(tag: string): Observable<string> {
    return this.http.get<string>(this.prGetPatternViewUrl, {params: {tag}}).pipe(
      catchError( err => {
        if (err.status !== '500') {
          return throwError( `Error task preview ${err.error}`);
        } else {
          return throwError( `Error task preview ${err.error} (status ${err.status})`);
        }
      }));
  }
  getTaskTemplate(taskID: string): Observable<Partial<TaskTemplate>> {
    return this.http.get<Partial<TaskTemplate>>(this.prTaskTemplateUrl, {params: {task_id: taskID }});
  }

  createTaskTemplate(taskTemplate: TaskTemplate, taskID: string, description: string, sourceAction: string): Observable<string> {
    return this.http.post<string>(this.prCreateTaskTemplateUrl, {taskTemplate, taskID, description, sourceAction}).pipe(
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

  getAllActiveTemplates(status= 'ACTIVE'): Observable<TemplateBase[]> {
    return this.http.get<TemplateBase[]>(this.prGetAllTemplateUrl, {params: {status}});
  }
  getAnalysisRequest(requestID: string): Observable<AnalysisSlice[]> {
    return this.http.get<AnalysisSlice[]>(this.prGetAnalysisRequestUrl, {params: {request_id: requestID}});
  }

  getAnalysisRequestStats(requestID: string): Observable<AnalysisRequestStats> {
    return this.http.get<AnalysisRequestStats>(this.prGetAnalysisRequestStatsUrl, {params: {request_id: requestID}});
  }
  getAnalysisRequestOutputs(requestID: string): Observable<string[]> {
    return this.http.get<string[]>(this.prGetAnalysisRequestOutputsUrl, {params: {request_id: requestID}});
  }
  saveTemplateParams(templateID: string, templateBase: Partial<TemplateBase>|null,  params: PatternChanges): Observable<any> {
    return this.http.post(this.prSaveTaskTemplateUrl, {templateID, templateBase,  params});
  }

  setProductionRequestStatus(requestID: string, status: string): Observable<any> {
    return this.http.post(this.prAnalysisRequestActionUrl, {requestID, action: 'setStatus', status});
  }
  createAnalysisRequest(description: string, requestExtID: string, templateBase: Partial<TemplateBase>, inputContainers: string[]): Observable<string> {
    return this.http.post<string>(this.prCreateAnalysisRequestUrl, {description, requestExtID, templateBase, inputContainers});
  }

  submitAnalysisRequestAction(requestID: string, action: string, slices: number[]): Observable<AnalysisRequestActionResponse> {
    return this.http.post<AnalysisRequestActionResponse>(this.prAnalysisRequestActionUrl, {requestID, action, slices}).pipe(
      catchError( err => {
         const result: AnalysisRequestActionResponse = {result: 'Error', error:  `Backend returned code ${err.status}, body was: ${err.error}`};
         return of(result);
      })
    );
  }
    getSlicesCommonTemplate(requestID: string, slices: number[]): Observable<AnalysisModificationActionResponse> {
      return this.http.post<AnalysisModificationActionResponse>(this.prAnalysisRequestActionUrl,
        {requestID, action: 'getSlicesTemplate', slices}).pipe(
        catchError( err => {
           const result: AnalysisModificationActionResponse = {template: null, slicesToModify: [], error:  `Backend returned code ${err.status}, body was: ${err.error}`};
           return of(result);
        })
      );
    }

    modifySlicesTemplate(requestID: string, slices: number[], templateBase: Partial<TaskTemplate>,
                         inputDataset = ''): Observable<AnalysisRequestActionResponse> {
    return this.http.post<AnalysisRequestActionResponse>(this.prAnalysisRequestActionUrl,
      {requestID, action: 'modifySlicesTemplate', slices, template: templateBase, inputDataset}).pipe(
      catchError( err => {
         const result: AnalysisRequestActionResponse = {result: null,
           error:  `Backend returned code ${err.status}, body was: ${err.error}`};
         return of(result);
      })
    );
  }

  getParentDerivation(parentRequest: string): Observable<ParentDerivationRequest> {
    return this.http.get<ParentDerivationRequest>(this.prGetParentDerivationUrl, {params: {requestID: parentRequest}});
  }

  createAnalysisRequestFromSlices(description: string, requestExtID: string, templateBase: Partial<TemplateBase>,
                                  inputSlices: {slice: number, outputFormat: string, requestID: string,
                                    container: string}[]): Observable<string> {
    return this.http.post<string>(this.prCreateAnalysisRequestUrl, {description, requestExtID, templateBase, inputSlices});
  }

  getAnalysisRequestHashtags(requestID: string): Observable<string[]> {
    return this.http.get<string[]>(this.prGetAnalysisRequestHashtagsUrl, {params: {requestID}});
  }

  addAnalysisRequestHashtag(requestID: string, hashtag: string) {
        return this.http.post<string>(this.prAddAnalysisRequestHashtagUrl, {requestID, hashtag, action: 'add'});
  }
  removeAnalysisRequestHashtag(requestID: string, hashtag: string) {
        return this.http.post<string>(this.prAddAnalysisRequestHashtagUrl, {requestID, hashtag, action: 'delete'});
  }
}
