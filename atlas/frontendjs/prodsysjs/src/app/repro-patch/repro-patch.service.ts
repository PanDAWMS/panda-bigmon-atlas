import {inject, Injectable} from '@angular/core';
import {BehaviorSubject, combineLatestWith, EMPTY, merge, Observable, of, Subject} from "rxjs";
import {HttpClient} from "@angular/common/http";
import {catchError, map, switchMap, tap} from "rxjs/operators";
import {setErrorMessage} from "../dsid-info/dsid-info.service";
import {takeUntilDestroyed} from "@angular/core/rxjs-interop";
import {signalSlice} from "ngxtension/signal-slice";
import {ProductionRequestBase} from "../production-request/production-request-models";

interface TaskToFix {
    original_task_id: number;
    slices_to_clone: SliceToClone[];
    tasks_to_abort: number[];
    container: string;
    error: string;
}

interface SliceToClone {
    slice_id: number;
    steps_number: number;
    replace_first_step: boolean;
}

interface TaskPatchData {
    patchedTasks: number[];
    tasksToFix: TaskToFix[];
    request: ProductionRequestBase;
}


export interface ReproPatchState {
  isLoading: boolean;
  taskPatchData: TaskPatchData | undefined;
  requestID: string | undefined;
  selectedTask: string | undefined;
  error: string | null;
  patched: boolean;
}
export interface PatchData {
  taskPatchData: TaskPatchData;
  requestID: string;
}
@Injectable({
  providedIn: 'root'
})
export class ReproPatchService {

  private prGetPatchInfoUrl = '/api/reprocessing_request_patch_info';
  private prPostPatchUrl = '/api/patch_reprocessing_request';
  private http = inject(HttpClient);


  private initialState: ReproPatchState = {
    isLoading: false,
    taskPatchData: undefined,
    requestID: undefined,
    selectedTask: undefined,
    error: null,
    patched: false
  };
  private error$ = new Subject<string>();
  private isLoading$ = new Subject<boolean>();
  private requestID$ = new Subject<string>();
  private selectedTask$ = new Subject<string>();
  private patched$ = new Subject<boolean>();
  private requestInfo$ = this.requestID$.pipe(combineLatestWith(this.selectedTask$),
    tap(() => this.isLoading$.next(true)),
    switchMap(([requestID, selectedTask]) => this.repoPatchLoad(requestID, selectedTask)),
    tap(() => this.isLoading$.next(false)),
    takeUntilDestroyed()
  );

  sources$ = merge(
    this.requestInfo$.pipe(map(taskPatchData => ({taskPatchData}))),
    this.error$.pipe(map(error => ({error}))),
    this.isLoading$.pipe(map(isLoading => ({isLoading}))),
    this.requestID$.pipe(map(requestID => ({requestID}))),
    this.patched$.pipe(map(patched => ({patched})))
  );
  state = signalSlice({
    initialState: this.initialState,
    sources: [this.sources$],
    selectors: (state) => ({
      // count number of tasks to abort
      tasksToAbort: () => state().taskPatchData?.tasksToFix.reduce((acc, task) => acc + task.tasks_to_abort.length, 0),
      containers: () => state().taskPatchData?.tasksToFix.map(task => task.container),
    }),
  });
  repoPatchLoad(requestID: string, selectedTask: string = ''): Observable<TaskPatchData> {
    return this.http.get<TaskPatchData>(`${this.prGetPatchInfoUrl}/${requestID}/`, {params: {selectedTask}}).pipe(
      catchError((error: any) => {
        this.error$.next(setErrorMessage(error));
        this.isLoading$.next(false);
        return EMPTY;
      })
    );
  }
  repoPatchRequest(requestID: string, tasksToPatch: number[], amiTag: string): Observable<number> {
    return this.http.post<number>(`${this.prPostPatchUrl}/${requestID}/`, {tasksToPatch, amiTag}).pipe(
      catchError((error: any) => {
        this.error$.next(setErrorMessage(error));
        this.isLoading$.next(false);
        return of(0);
      })
    );
  }


  public setRequestID(requestID: string): void {
      this.requestID$.next(requestID);
  }

    public setSelectedTask(selectedTask: string): void {
      this.selectedTask$.next(selectedTask);
  }

  public applyPatch(amiTag): void {
    const requestID = this.state().requestID;
    // take only original_task_id list
    const tasks = this.state().taskPatchData.tasksToFix.map(task => task.original_task_id);
    this.isLoading$.next(true);
    this.repoPatchRequest(requestID, tasks, amiTag).subscribe((patched) => {
      this.isLoading$.next(false);
      this.patched$.next(patched > 0);
    });
  }


  constructor() { }
}
