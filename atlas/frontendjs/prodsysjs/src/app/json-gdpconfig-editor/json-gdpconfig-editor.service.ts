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
    error: string | null;
}

@Injectable({
  providedIn: 'root'
})
export class JsonGdpconfigEditorService {

  private prGetGDPJsonParamURL = '/gdpconfig/get_json_param/';
  private http = inject(HttpClient);
  private state = signal<JSONGDPConfigEditorState>({
    isLoading: false,
    value: undefined,
    key: undefined,
    error: null
  });

  private selectedKey$ = new Subject<string>();

  isLoading = computed(() => this.state().isLoading);
  value = computed(() => this.state().value);
  errorMessage = computed(() => this.state().error);



  constructor() {
    this.selectedKey$.pipe(
      tap(() => this.setLoadingIndicator(true)),
      tap(key => this.setKeyState(key)),
      switchMap(key => this.getGDPJsonParam(key)),
      takeUntilDestroyed()
    ).subscribe(value => this.setValue(value));
  }

  public setKey(key: string): void {
    this.selectedKey$.next(key);
  }
  getGDPJsonParam(key: string): Observable<any> {
    return this.http.get(this.prGetGDPJsonParamURL, {params: {key}}).pipe(catchError(err => this.setError(err)));
  }

  private setLoadingIndicator(isLoading: boolean): void {
    this.state.update(state => ({
      ...state,
      isLoading
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
