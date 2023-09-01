import { Injectable } from '@angular/core';
import { Router, RouterStateSnapshot, ActivatedRouteSnapshot } from '@angular/router';
import { Observable, of } from 'rxjs';
import {UnmergeCleaningService, UnmergedDatasetsCombined} from "./unmerge-cleaning.service";

@Injectable({
  providedIn: 'root'
})
export class UnmergeCleaningResolver  {
  constructor(private service: UnmergeCleaningService) {
  }
  resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<UnmergedDatasetsCombined>|Promise<any>|any {
    return this.service.getUnmergeDatasets(route.paramMap.get('prefix'));
  }
}

@Injectable({
  providedIn: 'root'
})
export class UnmergeNotDeletedResolver  {
  constructor(private service: UnmergeCleaningService) {
  }
  resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<UnmergedDatasetsCombined>|Promise<any>|any {
    return this.service.getUnmergeNotDeletedDatasets(route.paramMap.get('prefix'));
  }
}

@Injectable({
  providedIn: 'root'
})
export class SpecialCleaningResolver  {
  constructor(private service: UnmergeCleaningService) {
  }
  resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<UnmergedDatasetsCombined>|Promise<any>|any {
    return this.service.getSpecialDatasets(route.paramMap.get('parentTag'), route.paramMap.get('childTag'));
  }
}
