import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
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
  private cache$: Observable<UnmergedDatasetsCombined>;
  constructor(private http: HttpClient) { }

  getUnmergeDatasets(prefix: string): Observable<UnmergedDatasetsCombined>{
    if (!this.cache$) {
      this.cache$ = this._getUnmergeDatasets(prefix).pipe(
        shareReplay(CACHE_SIZE)
      );
    }

    return this.cache$;
  }

  private _getUnmergeDatasets(prefix: string): Observable<UnmergedDatasetsCombined> {
    return this.http.get<UnmergedDatasetsCombined>(this.unmergeDatasetURL, {params: {'prefix': prefix}});
  }
}
