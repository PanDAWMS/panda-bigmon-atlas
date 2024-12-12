import {computed, inject, Injectable, signal} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Observable, of, Subject} from 'rxjs';
import {setErrorMessage} from '../dsid-info/dsid-info.service';
import {catchError, switchMap, tap} from 'rxjs/operators';
import {takeUntilDestroyed} from '@angular/core/rxjs-interop';


class JSONGDPConfigEditorState {
    isLoading: boolean;
    value: any | undefined;
    key: string | undefined;
    saved: boolean;
    error: string | null;
}

@Injectable({
  providedIn: 'root'
})
export class JsonGdpconfigEditorService {

  private prGetGDPJsonParamURL = '/gdpconfig/get_json_param/';
  private prSetGDPJsonParamURL = '/gdpconfig/save_json_param/';
  private http = inject(HttpClient);
  private state = signal<JSONGDPConfigEditorState>({
    isLoading: false,
    value: undefined,
    key: undefined,
    saved: false,
    error: null
  });

  private selectedKey$ = new Subject<string>();
  private saveKey$ = new Subject<{key: string, value: any}>();

  isLoading = computed(() => this.state().isLoading);
  value = computed(() => this.state().value);
  errorMessage = computed(() => this.state().error);
  saved = computed(() => this.state().saved);



  constructor() {
    this.selectedKey$.pipe(
      tap(() => this.setLoadingIndicator(true)),
      tap(key => this.setKeyState(key)),
      switchMap(key => this.getGDPJsonParam(key)),
      takeUntilDestroyed()
    ).subscribe(value => this.setValue(value));
    this.saveKey$.pipe(
      tap(() => this.setSavedState(false)),
      tap(() => this.setLoadingIndicator(true)),
      switchMap(({key, value}) => this.saveGDPJsonParam(key, value)),
      tap(() => this.setLoadingIndicator(false)),
      tap(() => this.setSavedState(true)),
      takeUntilDestroyed()
    ).subscribe(value => this.setValue(value));
  }

  public setKey(key: string): void {
    this.selectedKey$.next(key);
  }
  public saveKey(key: string, value: any): void {
    this.saveKey$.next({key, value});
  }
  public cleanSavedState(): void {
    this.setSavedState(false);
  }
  getGDPJsonParam(key: string): Observable<any> {
    return this.http.get(this.prGetGDPJsonParamURL, {params: {key}}).pipe(catchError(err => this.setError(err)));
  }
  saveGDPJsonParam(key: string, value: any): Observable<any> {
    return this.http.post(this.prSetGDPJsonParamURL, {key, value}).pipe(catchError(err => this.setError(err)));
  }
  private setLoadingIndicator(isLoading: boolean): void {
    this.state.update(state => ({
      ...state,
      isLoading
    }));
  }

  private setSavedState(saved: boolean): void {
    this.state.update(state => ({
      ...state,
      saved
    }));
  }

  private setKeyState(key: string): void {
    this.state.update(state => ({
      ...state,
      key
    }));
  }

  private setValue(value: any): void {
    this.state.update(state => ({
      ...state,
      value,
      isLoading: false
    }));
  }

  private setError(err: any): Observable<any | undefined> {
      const errorMessage = setErrorMessage(err);
      this.state.update(state => ({
        ...state,
        error: errorMessage
      }));
      return of(undefined);
  }

}
