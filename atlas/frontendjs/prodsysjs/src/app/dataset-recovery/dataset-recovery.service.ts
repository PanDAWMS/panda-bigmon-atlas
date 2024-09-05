import {inject, Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {EMPTY, merge, Observable, Subject} from 'rxjs';
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
  submitted: boolean;
}

@Injectable({
  providedIn: 'root'
})
export class DatasetRecoveryService {

  private prGetDatasetRecoveryUrl = '/api/unavailable_datasets_info';
  private http = inject(HttpClient);

  private initialState: DatasetRecoveryState = {
    isLoading: false,
    tsData: undefined,
    error: null,
    submitted: false
  };

  private error$ = new Subject<string>();
  private isLoading$ = new Subject<boolean>();
  private submitted$ = new Subject<boolean>();
  private username$ = new Subject<string>();
  private datasetsInfo$ = this.username$.pipe(
    tap(() => this.isLoading$.next(true)),
    switchMap(username => this.getDatasetRecoveryInfo(username)),
    tap(() => this.isLoading$.next(false)),
    takeUntilDestroyed()
  );

  sources$ = merge(
    this.error$.pipe(map(error => ({error}))),
    this.isLoading$.pipe(map(isLoading => ({isLoading}))),
    this.submitted$.pipe(map(submitted => ({submitted}))),
    this.datasetsInfo$.pipe(map(tsData => ({tsData})))
  );

  state = signalSlice({
    initialState: this.initialState,
    sources: [this.sources$]
  });

  getDatasetRecoveryInfo(username: string): Observable<TSData> {
    return this.http.get<TSData>(this.prGetDatasetRecoveryUrl, {params: {username}}).pipe(
      catchError((error: any) => {
        this.error$.next(setErrorMessage(error));
        this.isLoading$.next(false);
        return EMPTY;
      })
    );
  }



  constructor() { }

  setUsername(value: string) {
    this.username$.next(value);
  }

  submit() {

  }
}
