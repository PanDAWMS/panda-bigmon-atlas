import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Observable, of} from 'rxjs';



export interface AMITag {
  cache: string;
  skim: string;
}

@Injectable({
  providedIn: 'root'
})
export class AmiTagService {

  private amiTagsDetailsUrl = '/gpdeletion/ami_tags_details';
  constructor(
    private http: HttpClient){}

  getAMITagDetails(amiTags: string): Observable<Map<string, AMITag>> {
    return this.http.get<Map<string, AMITag>>(this.amiTagsDetailsUrl, {params: {ami_tags: amiTags }});
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
    console.log(`AmiTagService: ${message}`);
  }
}
