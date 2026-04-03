import { Injectable, inject } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';
import { Observable } from 'rxjs';
import {ContainerAllInfo, GpContainerInfoService} from "../gp-container-info.service";


@Injectable({ providedIn: 'root' })
export class GpContainerInfoResolver  {
  private service = inject(GpContainerInfoService);


  resolve(
    route: ActivatedRouteSnapshot,
  ): Observable<any>|Promise<any>|any {
    return this.service.getContainerFullDetails(route.paramMap.get('container'));
  }
}
