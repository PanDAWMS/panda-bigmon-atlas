import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable} from "rxjs";


export interface DerivationDAODDerivation {
       campaign: string;
        subcampaign: string;
        outputs: string[];
        train_id?: number;
        request_id: number;
}

export interface MCCampaign {
        campaign: string;
        subcampaigns: string[];
}

export interface PatternStep {
  ami_tag: string;
  outputs: string[];
  project_mode: string;
}
@Injectable({
  providedIn: 'root'
})
export class DerivationPhysPatternService {


  constructor(private http: HttpClient) { }

  getPatternWithCampaigns(): Observable<{current_patterns: DerivationDAODDerivation[], mc_campaigns: MCCampaign[], steps: PatternStep[][] }> {
    return this.http.get<{current_patterns: DerivationDAODDerivation[], mc_campaigns: MCCampaign[], steps: PatternStep[][] }>('/prodtask/get_derivation_phys_pattern');
  }
  setPatternByRequestID(patterns: DerivationDAODDerivation[]): Observable<PatternStep[][]> {
    return this.http.post<PatternStep[][]>('/prodtask/save_derivation_phys_pattern/',  {patterns});
  }

}
