import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable} from "rxjs";


export interface PatternStep {
  slice: number;
  ami_tag: string;
  output_formats: string[];
}
@Injectable({
  providedIn: 'root'
})
export class DerivationExtensionService {

  constructor(private http: HttpClient) { }
  private prDerivationPatternForExtUrl = '/api/form_pattern_for_derivation_request_extension/';
  private prDerivationExtRequestUrl = '/api/extend_derivation_request/';





  getDerivationExtensionInfo(requestID: string, slices: number[]): Observable<PatternStep[]> {
    return this.http.post<PatternStep[]>(this.prDerivationPatternForExtUrl, {requestID, slices});
  }

  extendDerivationRequest(requestID: string, slices: number[], containerList: string[]): Observable<string> {
    return this.http.post<string>(this.prDerivationExtRequestUrl, {requestID, slices, containerList});
  }
}
