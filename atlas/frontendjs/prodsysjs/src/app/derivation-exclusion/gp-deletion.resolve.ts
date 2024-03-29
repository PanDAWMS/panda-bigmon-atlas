import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';
import { Observable } from 'rxjs';
import {GroupProductionDeletionContainer} from './gp-deletion-container';
import {GPDeletionContainerService} from './gp-deleation.service';


@Injectable({ providedIn: 'root' })
export class GPDeletionContainerResolver  {
  constructor(private service: GPDeletionContainerService) {}

  resolve(
    route: ActivatedRouteSnapshot,
  ): Observable<any>|Promise<any>|any {
    return this.service.getGPDeletionPerOutput(route.paramMap.get('output'), route.paramMap.get('data_type'));
  }
}
