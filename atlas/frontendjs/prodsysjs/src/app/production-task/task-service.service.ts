import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {BehaviorSubject, Observable, of, Subject} from 'rxjs';
import {JEDITask, ProductionTask, Slice} from '../production-request/production-request-models';
import {GroupProductionStats} from '../derivation-exclusion/gp-stats/gp-stats';
import {catchError, filter, map, shareReplay, switchMap, tap} from 'rxjs/operators';

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

export interface TaskHS06 {
  finished: number;
  failed: number;
  running: number;
  parentPercent: number;
  total_output_size: number;
  input_events: number;
  input_bytes: number;
}

export type ActionParams = number[]|string[]|boolean[]|null;

export interface TaskAction {
  tasks: ProductionTask[];
  action: string;
  params: ActionParams;
  comment: string;
  action_name: string;
  params_name: string[]|null;

}

export interface TaskActionResult{
  action_sent: boolean;
  result: {task_id: number, return_code: string, return_info: string}[]|null;
  action_verification: {id: number, action_allowed: boolean, user_allowed: boolean}[]|null;
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
  private prTaskActionUrl = '/api/tasks_action/';
  private prTaskReassignEntitiesUrl = '/production_request/reassign_entities/';
  private prTaskStatsUrl = '/production_request/production_task_hs06/';
  private prErrorLogsUrl = '/production_request/production_error_logs/';
  private prTaskExtensionUrl = '/production_request/production_task_extensions/';



  private reassignCache$: Observable<ReassignDestination>;
  private actionSubject$: Subject<TaskAction|null> = new Subject();
  private actionResults$: BehaviorSubject<TaskActionResult|null> = new BehaviorSubject(null);
  private requestReassignEntities(): Observable<ReassignDestination>  {
    return this.http.get<ReassignDestination>(this.prTaskReassignEntitiesUrl)
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
          result = {error: `server problem with task loading ${err.error} `};
        } else {
          result = {error: `server problem with task loading ${err.error} (status ${err.status})`};
        }
        return of(result);
      })
    );
  }

  getTaskActionLogs(id: string): Observable<TaskActionLog[]> {
    return this.http.get<TaskActionLog[]>(this.prTaskActionsUrl, {params: {task_id: id }});
  }

  getTaskStats(id: string): Observable<TaskHS06> {
    return this.http.get<TaskHS06>(this.prTaskStatsUrl, {params: {task_id: id }});
  }
  getTaskErrorLogs(id: string): Observable<{log: string|null}> {
    return this.http.get<{log: string|null}>(this.prErrorLogsUrl, {params: {task_id: id }});
  }
  getTaskExtension(id: string): Observable<{id: number, status: string}[]|[]> {
    return this.http.get<{id: number, status: string}[]|[]>(this.prTaskExtensionUrl, {params: {task_id: id }});
  }
  getActionResults(): Observable<TaskActionResult|null>{
    return this.actionResults$;
  }

  submitTaskAction(tasksID: number[], action: string, comment: string,
                   params: number[]|string[]|boolean[]|null): Observable<TaskActionResult>{
    return this.http.post<TaskActionResult>(this.prTaskActionUrl, {tasksID, action, comment, params}).pipe(
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
    console.log(`TaskService: ${message}`);
  }

}
