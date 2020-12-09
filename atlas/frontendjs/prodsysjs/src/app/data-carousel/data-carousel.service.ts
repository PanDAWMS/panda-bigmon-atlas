import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';

import { Observable, of } from 'rxjs';
import { catchError, map, tap } from 'rxjs/operators';

import {RequestPerDay} from './request-per-day';

@Injectable({
  providedIn: 'root'
})
export class DataCarouselService {
    private requestPerDayUrl = '/prestage/derivation_requests';
  constructor(
    private http: HttpClient){}

  getRequestsPerDay(): Observable<RequestPerDay[]> {
    return this.http.get<RequestPerDay[]>(this.requestPerDayUrl)
      .pipe(
        tap(_ => this.log('fetched requests per day')),
        catchError(this.handleError<RequestPerDay[]>('getRequestsPerDay', []))
      );
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
    console.log(`DataCarouselService: ${message}`);
  }

}

