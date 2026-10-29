import {inject, Injectable} from '@angular/core';
import {HttpClient, HttpErrorResponse} from '@angular/common/http';
import {Observable} from 'rxjs';
import {GlobalShareTree} from './global-shares.models';

@Injectable({providedIn: 'root'})
export class GlobalSharesService {
  private readonly http = inject(HttpClient);

  getTree(): Observable<GlobalShareTree> {
    return this.http.get<GlobalShareTree>('/gdpconfig/global_share_tree_v2/');
  }

  changeShares(shares: Record<string, number>): Observable<{result: string; exception?: string}> {
    return this.http.post<{result: string; exception?: string}>('/gdpconfig/global_share_change/', shares);
  }
}

export function globalShareError(error: unknown): string {
  if (error instanceof HttpErrorResponse) {
    return error.error?.detail || error.error?.exception ||
      (typeof error.error === 'string' ? error.error : error.message);
  }
  return error instanceof Error ? error.message : 'Unable to load global shares.';
}
