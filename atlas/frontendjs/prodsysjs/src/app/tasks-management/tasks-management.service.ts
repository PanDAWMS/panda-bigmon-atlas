import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable, of, throwError} from "rxjs";
import {TaskInfo} from "../production-task/task-service.service";
import {ProductionRequestBase, ProductionTask} from "../production-request/production-request-models";
import {catchError} from "rxjs/operators";

@Injectable({
  providedIn: 'root'
})
export class TasksManagementService {

  constructor(private http: HttpClient) { }
  private prTasksByRequestUrl = '/production_request/production_task_for_request/';
  private prTasksPrRequestUrl = '/production_request/production_request_info';

  getProductionRequest(requestID: string): Observable<ProductionRequestBase> {
    return this.http.get<ProductionRequestBase>(this.prTasksPrRequestUrl, {params: {prodcution_request_id: requestID}});
  }

  getTasksByRequestSlices(requestID: string, slices: number[]|null): Observable<ProductionTask[]> {
    return this.http.post<ProductionTask[]>(this.prTasksByRequestUrl , {requestID, slices}).pipe(
      catchError( err => {
        if (err.status !== '400') {
          return throwError( `Error task loading ${err.error}`);
        } else {
          return throwError( `Error task loading ${err.error} (status ${err.status})`);
        }
      }));
  }

  getTasksByHashtag(hashtagString: string): Observable<ProductionTask[]> {
    return this.http.post<ProductionTask[]>(this.prTasksByRequestUrl , {hashtagString});
  }
}