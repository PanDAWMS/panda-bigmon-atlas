import {inject, Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {BehaviorSubject, combineLatestWith, EMPTY, merge, Observable, Subject} from 'rxjs';
import {catchError, map, switchMap, tap} from 'rxjs/operators';
import {signalSlice} from 'ngxtension/signal-slice';
import {setErrorMessage} from '../dsid-info/dsid-info.service';
import {takeUntilDestroyed} from "@angular/core/rxjs-interop";

export interface Dataset {
  input_dataset: string;
  replicas: string[];
  size: number;
  task_id: number;
  status: string;
}

interface Downtime {
  endpoint: string;
  expiration: string;
  reason: string;
  status: string;
}

export interface TSData {
  datasets: Dataset[];
  downtimes: Downtime[];
}

export interface DatasetRecoveryState {
  isLoading: boolean;
  tsData: TSData | undefined;
  error: string | null;
  submitting: boolean;
  submitted: boolean;
}

export interface DatasetRequest {
  id: string;
  original_dataset: string;
  status: 'pending' | 'submitted' | 'running' | 'done';
  type: string;
  timestamp: string;
  requestor: string;
  submitter: string;
  size: string;
  sites: string;
  original_task: number;
  recovery_task: number | null;
  comment: string;
  error: string;
  containers: string[];
}

@Injectable({
  providedIn: 'root'
})
export class DatasetRecoveryService {

  private prGetDatasetRecoveryUrl = '/api/unavailable_datasets_info';
  private prSubmitDatasetRecoveryRequestUrl = '/api/request_recreation/';
  private prAllRequestUrl = '/api/get_all_recovery_requests/';
  private prSubmitRecoveryUrl = '/api/submit_recreation/';



  private http = inject(HttpClient);

  private initialState: DatasetRecoveryState = {
    isLoading: false,
    tsData: undefined,
    error: null,
    submitted: false,
    submitting: false
  };

  private error$ = new Subject<string>();
  private isLoading$ = new Subject<boolean>();
  private submitted$ = new BehaviorSubject<boolean>(false);
  private submitting$ = new Subject<boolean>();
  private inputValues$ = new Subject<{username: string, dataset: string, taskID: string}>();
  private datasetsInfo$ = this.inputValues$.pipe(
    combineLatestWith(this.submitted$),
    tap(() => this.isLoading$.next(true)),
    tap(() => this.error$.next(null)),
    switchMap(([inputValues, _]) => this.getDatasetRecoveryInfo(inputValues.username, inputValues.dataset, inputValues.taskID)),
    tap(() => this.isLoading$.next(false)),
    takeUntilDestroyed()
  );

  sources$ = merge(
    this.error$.pipe(map(error => ({error}))),
    this.isLoading$.pipe(map(isLoading => ({isLoading}))),
    this.submitted$.pipe(map(submitted => ({submitted}))),
    this.datasetsInfo$.pipe(map(tsData => ({tsData}))),
    this.submitting$.pipe(map(submitting => ({submitting})))
  );

  state = signalSlice({
    initialState: this.initialState,
    sources: [this.sources$]
  });

  getDatasetRecoveryInfo(username: string, dataset: string, taskID: string): Observable<TSData> {
    return this.http.get<TSData>(this.prGetDatasetRecoveryUrl, {params: {username, dataset, taskID}}).pipe(
      catchError((error: any) => {
        this.error$.next(setErrorMessage(error));
        this.isLoading$.next(false);
        return EMPTY;
      })
    );
  }

  getAllRequests(): Observable<DatasetRequest[]> {
    this.isLoading$.next(true);
    return this.http.get<DatasetRequest[]>(this.prAllRequestUrl).pipe(
      tap(() => this.isLoading$.next(false)),
      catchError((error: any) => {
        this.error$.next(setErrorMessage(error));
        this.isLoading$.next(false);
        return EMPTY;
      })
    );
  }

  constructor() { }

  setInputValues(username: string, dataset: string, taskID: string): void {
    this.inputValues$.next({username, dataset, taskID});
  }

  submit(datasets: Dataset[], comment: string): void {
    this.submitting$.next(true);
    this.http.post(this.prSubmitDatasetRecoveryRequestUrl, {datasets, comment}).pipe(
      catchError((error: any) => {
        this.error$.next(setErrorMessage(error));
        this.submitting$.next(false);
        return EMPTY;
      })
    ).subscribe(() => {
      this.submitted$.next(true);
      this.submitting$.next(false);

    });

  }

  submitRecovery(requestRequestsIDs: string[]): void {
    this.submitting$.next(true);
    this.http.post(this.prSubmitRecoveryUrl, {IDs: requestRequestsIDs}).pipe(
      catchError((error: any) => {
        this.error$.next(setErrorMessage(error));
        this.submitting$.next(false);
        return EMPTY;
      })
    ).subscribe(() => {
      this.submitted$.next(true);
      this.submitting$.next(false);

    });

  }
}
