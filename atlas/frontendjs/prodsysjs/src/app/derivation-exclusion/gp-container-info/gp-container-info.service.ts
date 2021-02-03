import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable, of} from "rxjs";
import {GroupProductionDeletionContainer} from "../gp-deletion-container";


export interface Dataset {
  name: string;
  events: number;
  bytes: number;
  task_id: number;
}

export interface ContainerContent {
  container: string;
  datasets: Dataset[];
  details: GroupProductionDeletionContainer;
}

export interface Extension  {
  id: number;
  timestamp: string;
  user: string;
  message: string;
}

export interface ContainerContent {
  container: string;
  datasets: Dataset[];
}

export interface ContainerAllInfo {
  main_container: ContainerContent;
  extension: Extension[];
  same_input: ContainerContent[];
}

@Injectable({
  providedIn: 'root'
})
export class GpContainerInfoService {

  private amiTagsDetailsUrl = '/gpdeletion/gp_container_details';
  constructor(
    private http: HttpClient){}

  getContainerFullDetails(containerName: string): Observable<ContainerAllInfo|undefined> {
    return this.http.get<ContainerAllInfo|undefined>(this.amiTagsDetailsUrl, {params: {container: containerName }});
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
    console.log(`GpContainerInfoService: ${message}`);
  }
}
