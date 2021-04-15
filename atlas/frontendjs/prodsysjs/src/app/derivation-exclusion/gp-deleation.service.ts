import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';

import { Observable, of } from 'rxjs';
import { catchError, map, tap } from 'rxjs/operators';

import {ExtensionRequest, GroupProductionDeletionContainer} from './gp-deletion-container';

@Injectable({
  providedIn: 'root'
})
export class GPDeletionContainerService {
    private gpDeletionUrl = '/gpdeletion/gpdeletions';
    private extensionUrl  = '/gpdeletion/extension/';
    private EXPIED_DAYS = 60;
  constructor(
    private http: HttpClient){}

  getGPDeletionPerOutput(outputType: string, dataType: string): Observable<GroupProductionDeletionContainer[]> {
    return this.http.get<GroupProductionDeletionContainer[]>(this.gpDeletionUrl, {params: {data_type: dataType,
         output_format: outputType}})
      .pipe(
        map(gpList => this.calculateDatasetAge(gpList)),
        tap(_ => this.log(`fetched containers for group ${outputType} ${dataType}`)),
        catchError(this.handleError<GroupProductionDeletionContainer[]>('getGPDeletionPerOutput', []))
      );
  }
  askExtension(extensionRequest: ExtensionRequest): Observable<ExtensionRequest> {
    return this.http.post<ExtensionRequest>(this.extensionUrl, extensionRequest)
      .pipe(
        tap(_ => this.log('Asked for extension')),
        catchError(this.handleError<ExtensionRequest>('askExtension'))
      );
  }
  calculateDatasetAge(gpList: GroupProductionDeletionContainer[]): GroupProductionDeletionContainer[] {
    const now = Date.now() / 1000;
    for (const gpContainer of gpList){
      gpContainer.age = (now - gpContainer.epoch_last_update_time) / (86400);
      let expiredDays = this.EXPIED_DAYS;
      if ((gpContainer.extensions_number !== undefined) && (gpContainer.extensions_number > 0)){
        expiredDays = this.EXPIED_DAYS + 60 * gpContainer.extensions_number;
        gpContainer.expended_till = (gpContainer.epoch_last_update_time + expiredDays * 86400) * 1000;
      }
      if (gpContainer.age > expiredDays){
        gpContainer.is_expired = 'expired';
      } else {
        gpContainer.is_expired = 'not_expired';
      }

    }
    return gpList;
  }
    private handleError<T>(operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {

      // TODO: send the error to remote logging infrastructure
      console.error(error); // log to console instead

      // TODO: better job of transforming error for user consumption
      this.log(`${operation} failed: ${error.message}`);

      // Let the app keep running by returning an empty result.
      return of(result as T);
    };
  }

  private log(message: string) {
    console.log(`GPDeletionContainerService: ${message}`);
  }

}
