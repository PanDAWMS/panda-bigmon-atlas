import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, RouterStateSnapshot } from '@angular/router';
import { Observable } from 'rxjs';
import {GroupProductionStats} from './gp-stats';
import {GPStatsService} from './gp-stats.service';



@Injectable({ providedIn: 'root' })
export class GPStatsResolver  {
  constructor(private service: GPStatsService) {}

  resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<any>|Promise<any>|any {
    return this.service.getGPStats();
  }
}
