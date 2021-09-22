import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable, of} from "rxjs";
import {DeletionSubmission} from "../gp-deletion-container";
import {catchError, tap} from "rxjs/operators";

@Injectable({
  providedIn: 'root'
})
export class GpDeletionRequestService {
  private gpDeletionRequestUrl = '/gpdeletion/gpdeletionrequests';
  private gpDeletionRequestSubmissionUrl = '/gpdeletion/gpdeletionrequestsask/';
  constructor(private http: HttpClient) { }

  getExistingDeletionRequests(): Observable<DeletionSubmission[]> {
    return this.http.get<DeletionSubmission[]>(this.gpDeletionRequestUrl).pipe(
      tap(_ => this.log('Fetched deletion requests')),
      catchError(this.handleError<DeletionSubmission[]>('getExistingDeletionRequests', []))
    );
  }

  postDeletionRequests(deadlineDate: Date, startDeletion: Date): Observable<DeletionSubmission> {
    return this.http.post<DeletionSubmission>(this.gpDeletionRequestSubmissionUrl,
      {deadline: deadlineDate, start_deletion: startDeletion}).pipe(
        tap(_ => this.log('Send deletion requests')),
        catchError(this.handleError<DeletionSubmission>('postDeletionRequests', undefined ))
      );
  }


  private handleError<T>(operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {

      // TODO: send the error to remote logging infrastructure
      console.error(error); // log to console instead

      // TODO: better job of transforming error for user consumption
      this.log(`${operation} failed: ${error.message}`);
      alert(`${operation} failed: ${error.message}`);
      // Let the app keep running by returning an empty result.
      return of(result as T);
    };
  }

  private log(message: string) {
    console.log(`GPDeletionContainerService: ${message}`);
  }
}
