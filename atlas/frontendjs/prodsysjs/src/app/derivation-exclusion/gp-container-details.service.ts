import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import {Observable, of} from 'rxjs';
import {catchError, tap} from 'rxjs/operators';
import {GroupProductionDeletionContainer} from './gp-deletion-container';


export interface GPContainerDetails {
  id: number;
  containers: GroupProductionDeletionContainer[];
}

@Injectable({
  providedIn: 'root'
})
export class GpContainerDetailsService {

    private gpDetailsUrl = '/gpdeletion/gpdetails';
  constructor(
    private http: HttpClient){}

  getGPContainerDetails(id: string): Observable<GPContainerDetails> {
     return this.http.get<GPContainerDetails>(this.gpDetailsUrl, {params: {gp_id: id }});
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

  private log(message: string): void {
    console.log(`GpContainerDetailsService: ${message}`);
  }

}
