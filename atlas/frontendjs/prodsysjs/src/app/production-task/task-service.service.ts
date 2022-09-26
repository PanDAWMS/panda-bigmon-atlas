import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Observable, of} from 'rxjs';
import {ProductionTask, Slice} from '../production-request/production-request-models';
import {GroupProductionStats} from '../derivation-exclusion/gp-stats/gp-stats';
import {catchError, shareReplay, tap} from 'rxjs/operators';

const CACHE_SIZE = 1;
export interface TaskActionLog {
  task_id: number;
  action: string;
  message: string;
}



export interface TaskActionResult{
  action_sent: boolean;
  result: {task_id: number, return_code: string, return_info: string}[]|null;
  action_verification: {task_id: number, action_allowed: boolean, user_allowed: boolean}[]|null;
}
export interface ReassignDestination{
  sites: string[];
  nucleus: string[];
  shares: string[];
}

@Injectable({
  providedIn: 'root'
})
export class TaskService {

  constructor(private http: HttpClient) { }
  private prTaskUrl = '/production_request/task';
  private prTaskActionsUrl = '/production_request/task_action_logs';
  private prTaskAction = '/api/tasks_action/';
  private prTaskReassignEntities = '/production_request/reassign_entities/';
  private reassignCache$: Observable<ReassignDestination>;

  private requestReassignEntities(): Observable<ReassignDestination>  {
    return this.http.get<ReassignDestination>(this.prTaskReassignEntities)
      .pipe(
        tap(_ => this.log(`fetched stats `)),
        catchError(this.handleError<ReassignDestination>('requestReassignEntities', {sites: [], nucleus: [], shares: []}))
      );
  }
  getTask(id: string): Observable<ProductionTask> {
    return this.http.get<ProductionTask>(this.prTaskUrl, {params: {task_id: id }});
  }

  getTaskActionLogs(id: string): Observable<TaskActionLog[]> {
    return this.http.get<TaskActionLog[]>(this.prTaskActionsUrl, {params: {task_id: id }});
  }

  submitTaskAction(tasksID: number[], action: string, comment: string, params: number[]|string[]|boolean[]|null): Observable<TaskActionResult>{
    return this.http.post<TaskActionResult>(this.prTaskAction, {tasksID, action, comment, params});
  }

  getReassignEntities(): Observable<ReassignDestination> {
    if (!this.reassignCache$) {
        this.reassignCache$ = this.requestReassignEntities().pipe(
          shareReplay(CACHE_SIZE)
        );
      }
    return this.reassignCache$;
  }

  private handleError<T>(operation = 'operation', result?: T): any {
    return (error: any): Observable<T> => {

      // TODO: send the error to remote logging infrastructure
      console.error(error); // log to console instead

      // TODO: better job of transforming error for user consumption
      this.log(`${operation} failed: ${error.message}`);

      // Let the app keep running by returning an empty result.
      return of(result as T);
    };
  }

  private log(message: string): void {
    console.log(`GPDeletionContainerService: ${message}`);
  }

}
