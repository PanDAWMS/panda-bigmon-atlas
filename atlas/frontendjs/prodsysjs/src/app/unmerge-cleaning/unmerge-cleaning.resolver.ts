import { Injectable } from '@angular/core';
import {
  Router, Resolve,
  RouterStateSnapshot,
  ActivatedRouteSnapshot
} from '@angular/router';
import { Observable, of } from 'rxjs';
import {UnmergeCleaningService, UnmergedDatasetsCombined} from "./unmerge-cleaning.service";

@Injectable({
  providedIn: 'root'
})
export class UnmergeCleaningResolver implements Resolve<UnmergedDatasetsCombined> {
  constructor(private service: UnmergeCleaningService) {
  }
  resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<UnmergedDatasetsCombined>|Promise<any>|any {
    return this.service.getUnmergeDatasets(route.paramMap.get('prefix'));
  }
}

@Injectable({
  providedIn: 'root'
})
export class SpecialCleaningResolver implements Resolve<UnmergedDatasetsCombined> {
  constructor(private service: UnmergeCleaningService) {
  }
  resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<UnmergedDatasetsCombined>|Promise<any>|any {
    return this.service.getSpecialDatasets(route.paramMap.get('parentTag'), route.paramMap.get('childTag'));
  }
}
