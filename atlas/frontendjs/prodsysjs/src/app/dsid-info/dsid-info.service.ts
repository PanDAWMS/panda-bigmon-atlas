import {computed, inject, Injectable, signal} from '@angular/core';
import {Observable, of, Subject} from "rxjs";
import {HttpClient, HttpErrorResponse} from "@angular/common/http";
import {catchError, filter, switchMap, tap} from "rxjs/operators";
import {takeUntilDestroyed} from "@angular/core/rxjs-interop";

interface ContainerDetail {
    container: string;
    datasets: string[];
    total_events: number;
    running_tasks: number;
}

interface CampaignContainer {
    [key: string]: ContainerDetail;
}

interface CampaignDetail {
    dsid: number;
    campaign: string;
    containers: CampaignContainer;
}

interface Containers {
    [key: string]: CampaignDetail;
}

interface EvgenDetail {
    campaign: string;
    total_events: number;
}

export interface DSIDObject {
    containers: Containers;
    evgen: EvgenDetail[];
}

class DSIDInfoState {
    isLoading: boolean;
    DSIDInfo: DSIDObject|undefined;
    DSID: number | undefined;
    error: string;
}

@Injectable({
  providedIn: 'root'
})
export class DSIDInfoService {

  private prGetDSIDInfoUrl = '/api/dsid_info/';
  private http = inject(HttpClient);

  private state = signal<DSIDInfoState>({
    isLoading: false,
    DSIDInfo: undefined,
    DSID: undefined,
    error: null
  });

  isLoading = computed(() => this.state().isLoading);
  dsidInfo = computed(() => this.state().DSIDInfo);
  errorMessage = computed(() => this.state().error);

  private selectedDSID$ = new Subject<number>();

  constructor() {
    this.selectedDSID$.pipe(
      filter(dsid => dsid > 0),
      tap(() => this.setLoadingIndicator(true)),
      tap(dsid => this.setDSID(dsid)),
      switchMap(dsid => this.getDSIDInfo(dsid)),
      takeUntilDestroyed()
    ).subscribe(DSIDInfo => this.setDSIDInfo(DSIDInfo));
  }
    private setLoadingIndicator(isLoading: boolean): void {
      this.state.update(state => ({
        ...state,
        isLoading
      }));
    }

    private setDSID(DSID: number): void {
      this.state.update(state => ({
        ...state,
        DSID
      }));
    }

    private setDSIDInfo(DSIDInfo: DSIDObject): void {
      this.state.update(state => ({
        ...state,
        DSIDInfo,
        isLoading: false
      }));
    }

  public setSelectedDSID(dsid: number): void {
    this.selectedDSID$.next(dsid);
  }
  getDSIDInfo(dsid: number): Observable<DSIDObject> {
    return this.http.get<DSIDObject>(this.prGetDSIDInfoUrl, {params: {dsid}}).pipe(
      catchError(err => this.setError(err))
    );
  }

  private setError(err: any): Observable<DSIDObject> {
      const errorMessage = setErrorMessage(err);
      this.state.update(state => ({
        ...state,
        error: errorMessage
      }));
      return of(null);
  }
}
export function setErrorMessage(err: HttpErrorResponse): string {
    let errorMessage: string;
    if (err.error instanceof ErrorEvent) {
      // A client-side or network error occurred. Handle it accordingly.
      errorMessage = `An error occurred: ${err.error.message}`;
    } else {
      // The backend returned an unsuccessful response code.
      // The response body may contain clues as to what went wrong,
      errorMessage = `Backend returned code ${err.status}: ${err.message} ${err.error}`;
    }
    console.error(err);
    return errorMessage;
}
