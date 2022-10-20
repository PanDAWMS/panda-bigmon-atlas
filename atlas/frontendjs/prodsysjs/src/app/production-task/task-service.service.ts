import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {BehaviorSubject, Observable, of, Subject} from 'rxjs';
import {JEDITask, ProductionTask, Slice} from '../production-request/production-request-models';
import {GroupProductionStats} from '../derivation-exclusion/gp-stats/gp-stats';
import {catchError, map, shareReplay, tap} from 'rxjs/operators';

const CACHE_SIZE = 1;
export interface TaskActionLog {
  task_id: number;
  action: string;
  return_message: string;
  return_code: string;
  username: string;
  params: string;
  timestamp: string;
  comment: string;
}
export type ActionParams = number[]|string[]|boolean[]|null;

export interface TaskAction {
  task: ProductionTask;
  action: string;
  params: ActionParams;
  comment: string;
  action_name: string;
  params_name: string[]|null;

}

export interface TaskActionResult{
  action_sent: boolean;
  result: {task_id: number, return_code: string, return_info: string}[]|null;
  action_verification: {task_id: number, action_allowed: boolean, user_allowed: boolean}[]|null;
  action?: string;
  tasksID?: number[];
  error?: string;
}
export interface ReassignDestination{
  sites: string[];
  nucleus: string[];
  shares: string[];
}

export interface TaskInfo {
  task?: ProductionTask;
  task_parameters?: any;
  job_parameters?: any;
  jedi_task?: JEDITask;
  output_datasets?: string[];
  error?: string;
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
  private actionSubject$: BehaviorSubject<TaskAction|null> = new BehaviorSubject(null);
  private actionResults$: BehaviorSubject<TaskActionResult|null> = new BehaviorSubject(null);
  private requestReassignEntities(): Observable<ReassignDestination>  {
    return this.http.get<ReassignDestination>(this.prTaskReassignEntities)
      .pipe(
        tap(_ => this.log(`fetched stats `)),
        catchError(this.handleError<ReassignDestination>('requestReassignEntities', {sites: [], nucleus: [], shares: []}))
      );
  }
  getTask(id: string): Observable<TaskInfo> {
    return this.http.get<TaskInfo>(this.prTaskUrl, {params: {task_id: id }}).pipe(
      catchError( err => {
        let result: TaskInfo;
        if (err.status !== '400') {
          result = {error: `Error task loading ${err.error} `};
        } else {
          result = {error: `Error task loading ${err.error} (status ${err.status})`};
        }
        return of(result);
      })
    );
  }

  getTaskActionLogs(id: string): Observable<TaskActionLog[]> {
    return this.http.get<TaskActionLog[]>(this.prTaskActionsUrl, {params: {task_id: id }});
  }

  getActionResults(): Observable<TaskActionResult|null>{
    return this.actionResults$;
  }

  submitTaskAction(tasksID: number[], action: string, comment: string,
                   params: number[]|string[]|boolean[]|null): Observable<TaskActionResult>{
    return this.http.post<TaskActionResult>(this.prTaskAction, {tasksID, action, comment, params}).pipe(
      map( result => {
        return {tasksID, action, action_sent: result.action_sent, result: result.result, action_verification: result.action_verification};
      }),
      catchError( err => {
        const result: TaskActionResult = {tasksID, action, action_sent: false, result: null, action_verification: null,
          error:  `Backend returned code ${err.status}, body was: ${err.error}`};
        return of(result);
      }),
      tap(result => this.actionResults$.next(result))
    );
  }

  getActionList(): Observable<TaskAction|null>{
    return this.actionSubject$;
  }

  addAction(taskAction: TaskAction): void{
    this.actionSubject$.next(taskAction);
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
