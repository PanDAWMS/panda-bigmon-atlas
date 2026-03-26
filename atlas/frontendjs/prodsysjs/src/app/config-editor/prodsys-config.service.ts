import {computed, inject, Injectable, Signal, signal} from '@angular/core';
import {HttpClient, HttpErrorResponse, httpResource} from "@angular/common/http";
import {rxResource, toObservable, toSignal} from "@angular/core/rxjs-interop";
import {setErrorMessage} from "../dsid-info/dsid-info.service";
import {JSONEditorOptions} from "jsoneditor";
import {catchError, Observable, of} from 'rxjs';
import {combineLatest, tap} from "rxjs/operators";

export interface ProdSysConfigParameter {
  name: string;
  value: any;
  description: string;
  schema: any;
}

@Injectable({
  providedIn: 'root'
})
export class ProdsysConfigService {

  private getConfigParamterUrl = '/api/config_parameter/';
  private setConfigParamterUrl = '/api/set_config_parameter/';


  private http = inject(HttpClient);

  private saveError = signal<HttpErrorResponse | null>(null);
  parameterName = signal<string|undefined>(undefined);

  constructor() { }

private configParameterResource = httpResource<ProdSysConfigParameter>(
  () => this.parameterName() ? `${this.getConfigParamterUrl}${this.parameterName()}` : undefined
);

configParameter = computed(() => this.configParameterResource.value() ?? undefined);
error = computed(() => this.configParameterResource.error() as HttpErrorResponse | null);
  errorMessage = computed(() => {
    if (this.error()) {
      return setErrorMessage(this.error());
    }else {
      return null;
    }
  });
  saveErrorMessage = computed(() => {
    if (this.saveError()){
      return setErrorMessage(this.saveError());
    }else {
      return null;
    }
  });
  isLoading = this.configParameterResource.isLoading;

  saveConfigParameter(value: any): Observable<any> {
    this.saveError.set(undefined);
    return this.http.post(this.setConfigParamterUrl + this.parameterName()+'/', value).pipe(
      tap(() => this.configParameterResource.reload()),
      catchError(error => {
        this.saveError.set(error);
        return of(null);
      })
    );
  }

}
