import {inject, Injectable} from '@angular/core';
import {EMPTY, merge, Observable, Subject} from "rxjs";
import {HttpClient} from "@angular/common/http";
import {catchError, map, switchMap, tap} from "rxjs/operators";
import {setErrorMessage} from "../dsid-info/dsid-info.service";
import {takeUntilDestroyed} from "@angular/core/rxjs-interop";
import {signalSlice} from "ngxtension/signal-slice";

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
}


export interface ReproPatchState {
  isLoading: boolean;
  taskPatchData: TaskPatchData | undefined;
  requestID: string | undefined;
  error: string | null;
  uploaded: boolean;
}

@Injectable({
  providedIn: 'root'
})
export class ReproPatchService {

  private prGetDSIDInfoUrl = '/api/reprocessing_request_patch_info/';
  private http = inject(HttpClient);


  private initialState: ReproPatchState = {
    isLoading: false,
    taskPatchData: undefined,
    requestID: undefined,
    error: null,
    uploaded: false
  };
  private error$ = new Subject<string>();
  private isLoading$ = new Subject<boolean>();
  private requestID$ = new Subject<string>();
  private requestInfo$ = this.requestID$.pipe(
    tap(() => this.isLoading$.next(true)),
    switchMap(requestID => this.repoPatchLoad(requestID)),
    tap(() => this.isLoading$.next(false)),
    takeUntilDestroyed()
  );

  sources$ = merge(
    this.requestInfo$.pipe(map(taskPatchData => ({taskPatchData}))),
    this.error$.pipe(map(error => ({error}))),
    this.isLoading$.pipe(map(isLoading => ({isLoading})))
  );
  state = signalSlice({
    initialState: this.initialState,
    sources: [this.sources$]
  });
  repoPatchLoad(requestID: string): Observable<TaskPatchData> {
    return this.http.get<TaskPatchData>(`${this.prGetDSIDInfoUrl}/${requestID}`).pipe(
      catchError((error: any) => {
        this.error$.next(setErrorMessage(error));
        return EMPTY;
      })
    );
  }

  public setRequestID(requestID: string): void {
      this.requestID$.next(requestID);
  }


  constructor() { }
}
