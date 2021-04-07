import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';

import { Observable, of } from 'rxjs';
import {catchError, map, shareReplay, tap} from 'rxjs/operators';
import {GroupProductionStats} from './gp-stats';

const CACHE_SIZE = 1;

@Injectable({
  providedIn: 'root'
})
export class GPStatsService {
    private gpStatsUrl = '/gpdeletion/gpstats';
    private gpLastUpdateTimeUrl = '/gpdeletion/last_update_time_group_production';
    private cache$: Observable<GroupProductionStats[]>;
  constructor(
    private http: HttpClient){}




  getGPStats(): Observable<GroupProductionStats[]> {
      if (!this.cache$) {
        this.cache$ = this.requestGPStats().pipe(
          shareReplay(CACHE_SIZE)
        );
      }

      return this.cache$;
  }

    private requestGPStats(): Observable<GroupProductionStats[]>  {
          return this.http.get<GroupProductionStats[]>(this.gpStatsUrl)
        .pipe(
          tap(_ => this.log(`fetched stats `)),
          catchError(this.handleError<GroupProductionStats[]>('getGPStats', []))
        );
    }
    GPLastUpdateTime(): Observable<string>  {
      return this.http.get<string>(this.gpLastUpdateTimeUrl)
        .pipe(
          tap(_ => this.log(`fetched update time `)),
          catchError(this.handleError<string>('GPLastUpdateTime', ''))
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
    console.log(`GPStatsService: ${message}`);
  }

}
