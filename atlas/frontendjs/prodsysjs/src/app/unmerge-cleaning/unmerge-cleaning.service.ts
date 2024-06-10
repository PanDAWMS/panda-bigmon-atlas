import { Injectable } from '@angular/core';
import { HttpClient } from "@angular/common/http";
import {Observable} from "rxjs";
import {DatasetToDelete} from "./unmerge-cleaning.component";
import {shareReplay} from "rxjs/operators";

const CACHE_SIZE = 1;
export interface UnmergedDatasetsCombined {
  timestamp: string;
  outputs: { [outputType: string]: DatasetToDelete[]};
}

@Injectable({
  providedIn: 'root'
})
export class UnmergeCleaningService {

  private unmergeDatasetURL = '/prodtask/unmerged_datasets_to_delete';
  private specialDatasetURL = '/prodtask/special_datasets_to_delete';
  private unmergeOldDatasetURL = '/prodtask/unmerge_datasets_not_deleted';
  private cache$: Map<string, Observable<UnmergedDatasetsCombined>> = new Map<string, Observable<UnmergedDatasetsCombined>>();
  private cache2$: Map<string, Observable<UnmergedDatasetsCombined>> = new Map<string, Observable<UnmergedDatasetsCombined>>();

  private cacheSpecial$: Observable<UnmergedDatasetsCombined>;
  constructor(private http: HttpClient) { }

  getUnmergeDatasets(prefix: string): Observable<UnmergedDatasetsCombined>{
    if (!this.cache$.has(prefix)) {
      this.cache$[prefix] = this._getUnmergeDatasets(prefix).pipe(
        shareReplay(CACHE_SIZE)
      );
    }

    return this.cache$[prefix];
  }

    getUnmergeNotDeletedDatasets(prefix: string): Observable<UnmergedDatasetsCombined>{
    if (!this.cache2$.has(prefix)) {
      this.cache2$[prefix] = this._getUnmergeNotDeletedDatasets(prefix).pipe(
        shareReplay(CACHE_SIZE)
      );
    }

    return this.cache2$[prefix];
  }
  getSpecialDatasets(parentTag: string, childTag: string): Observable<UnmergedDatasetsCombined>{
    if (!this.cache$.has(parentTag + childTag)) {
      this.cacheSpecial$ = this._getSpecialDatasets(parentTag,  childTag).pipe(
        shareReplay(CACHE_SIZE)
      );
    }

    return this.cacheSpecial$;
  }
  private _getUnmergeDatasets(prefix: string): Observable<UnmergedDatasetsCombined> {
    return this.http.get<UnmergedDatasetsCombined>(this.unmergeDatasetURL, {params: {'prefix': prefix}});
  }

  private _getUnmergeNotDeletedDatasets(prefix: string): Observable<UnmergedDatasetsCombined> {
    return this.http.get<UnmergedDatasetsCombined>(this.unmergeOldDatasetURL, {params: {'prefix': prefix}});
  }

  private _getSpecialDatasets(parentTag: string, childTag: string): Observable<UnmergedDatasetsCombined> {
    return this.http.get<UnmergedDatasetsCombined>(this.specialDatasetURL, {params: {parent_tag: parentTag, child_tag: childTag}});
  }
}
