import { Injectable } from '@angular/core';
import { HttpClient } from "@angular/common/http";
import {Observable} from "rxjs";


export interface PhysicsContainer {

    output_containers: string[];
    comment: string;
    super_tag: string;
    missing_containers: string[];
    not_full_containers: string[];
    name: string;
}

export interface PhysicsContainerIndex {
  containers: PhysicsContainer[];
  grl: string;
  grl_used: boolean;
}
@Injectable({
  providedIn: 'root'
})
export class DerivationPhysicContainerService {

  constructor(private http: HttpClient) { }
  private prPhysicsContainersURL  = '/api/physics_container_index/';
  private prCreatePhysicsContainersURL = '/api/create_physic_containers/';





  getPhysicContainers(requestID: string, grl: string): Observable<PhysicsContainerIndex> {
    return this.http.get<PhysicsContainerIndex>(this.prPhysicsContainersURL, {params: {requestID, grl}});
  }

  createPhysicsContainer(containers: PhysicsContainer[]): Observable<string[]> {
      return this.http.post<string[]>(this.prCreatePhysicsContainersURL, {containers});
  }

}

