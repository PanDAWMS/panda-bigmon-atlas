import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Observable} from 'rxjs';
import {DerivationContainersCollection} from './derivation-request-models';

@Injectable({
  providedIn: 'root'
})
export class DerivationFromTagService {

  constructor(private http: HttpClient) { }
  private prDerivationByTagUrl = '/production_request/derivation_input';



  getDerivationInputsByTag(amiTag: string): Observable<DerivationContainersCollection> {
    return this.http.get<DerivationContainersCollection>(this.prDerivationByTagUrl, {params: {ami_tag: amiTag }});
  }
}
