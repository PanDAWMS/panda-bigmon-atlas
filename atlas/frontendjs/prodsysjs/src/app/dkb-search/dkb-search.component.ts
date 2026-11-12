import {Component, DestroyRef, inject, OnInit} from '@angular/core';
import {CommonModule} from '@angular/common';
import {FormBuilder, ReactiveFormsModule} from '@angular/forms';
import {ActivatedRoute, Router} from '@angular/router';
import {takeUntilDestroyed} from '@angular/core/rxjs-interop';
import {catchError, distinctUntilChanged, map, of, switchMap} from 'rxjs';
import {AgGridAngular} from 'ag-grid-angular';
import {ColDef, GridApi, GridReadyEvent, SortChangedEvent} from 'ag-grid-community';
import {MatDialog} from '@angular/material/dialog';
import {DkbTaskDetailsComponent} from './dkb-task-details.component';
import {taskStatusStyle} from './dkb-task-status';
import {
  SubcampaignSummary, DkbSearchService, DkbTask, EMPTY_FILTERS, SearchFilters, SearchState, TaskPage,
} from './dkb-search.service';

@Component({
  selector: 'app-dkb-search',
  imports: [CommonModule, ReactiveFormsModule, AgGridAngular],
  templateUrl: './dkb-search.component.html',
  styleUrl: './dkb-search.component.css',
})
export class DkbSearchComponent implements OnInit {
  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private service = inject(DkbSearchService);
  private destroyRef = inject(DestroyRef);
  private dialog = inject(MatDialog);
  private gridApi?: GridApi<DkbTask>;
  form = inject(FormBuilder).nonNullable.group(EMPTY_FILTERS);
  state: SearchState = {...EMPTY_FILTERS, page: 1, page_size: 100, sort: 'task_id', direction: 'desc'};
  result: TaskPage | null = null;
  summary: SubcampaignSummary | null = null;
  tasksLoading = false;
  summaryLoading = false;
  tasksError = '';
  summaryError = '';
  readonly pageSizes = [100, 250, 500, 1000];
  readonly sortFields = [
    {value: 'task_id', label: 'Task ID'}, {value: 'pr_id', label: 'Request ID'},
    {value: 'taskname', label: 'Task name'}, {value: 'status', label: 'Status'},
    {value: 'campaign', label: 'Campaign'}, {value: 'subcampaign', label: 'Subcampaign'},
    {value: 'step_name', label: 'Step'}, {value: 'ctag', label: 'AMI tag'},
    {value: 'project', label: 'Project'}, {value: 'run_number', label: 'Run number'},
    {value: 'total_events', label: 'Total events'}, {value: 'processed_events', label: 'Processed events'},
    {value: 'requested_events', label: 'Requested events'}, {value: 'input_events', label: 'Input events'},
  ];
  readonly defaultColDef: ColDef = {
    resizable: true, sortable: true, sortingOrder: ['asc', 'desc'], filter: false, minWidth: 110,
    // The server orders all matches before pagination; retain that order within each page.
    comparator: () => 0,
  };
  readonly columns: ColDef<DkbTask>[] = [
    {field: 'taskid', colId: 'task_id', headerName: 'Task ID', width: 125,
      cellRenderer: params => {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'dkb-task-id';
        button.textContent = String(params.value);
        button.title = `Show DKB information for task ${params.value}`;
        button.addEventListener('click', () => this.showTask(params.value));
        return button;
      }},
    {headerName: 'Links', colId: 'links', width: 110, minWidth: 100, sortable: false,
      cellRenderer: params => this.taskLinks(params.data.taskid)},
    {field: 'taskname', headerName: 'Task name', minWidth: 320, flex: 1, tooltipField: 'taskname'},
    {field: 'pr_id', headerName: 'Request ID', width: 125,
      cellRenderer: params => this.link(params.value, '/prodtask/inputlist_with_request/')},
    {field: 'status', width: 130, cellStyle: params => taskStatusStyle(params.value)},
    {field: 'campaign'}, {field: 'subcampaign', minWidth: 150},
    {field: 'step_name', headerName: 'Step', minWidth: 150}, {field: 'ctag', headerName: 'AMI tag'},
    {field: 'project'}, {field: 'run_number', headerName: 'Run number', width: 140},
    ...(['total_events', 'processed_events', 'requested_events', 'input_events'] as const).map(field => ({
      field, headerName: field.replace(/_/g, ' '), minWidth: 160,
      valueFormatter: params => params.value == null ? '—' : Number(params.value).toLocaleString(),
    })),
  ];

  get pageCount(): number {
    return Math.max(1, Math.ceil((this.result?.available ?? 0) / this.state.page_size));
  }

  get canManageTasks(): boolean {
    return !this.tasksLoading && !!this.result && this.result.total > 0 && this.result.total < 10000;
  }

  manageTasks(): void {
    if (!this.canManageTasks) { return; }
    void this.router.navigate(['/tasks-by-dkb/dkb'], {queryParams: {
      ...this.filters(this.state), search: this.state.q || 'DKB Search', opensearch: '1',
    }});
  }

  showTask(taskID: number): void {
    this.dialog.open(DkbTaskDetailsComponent, {
      data: taskID, width: '1400px', maxWidth: '96vw', maxHeight: '90vh',
    });
  }

  get firstRow(): number {
    return this.result?.tasks.length ? (this.state.page - 1) * this.state.page_size + 1 : 0;
  }

  get lastRow(): number {
    return this.result?.tasks.length ? this.firstRow + this.result.tasks.length - 1 : 0;
  }

  ngOnInit(): void {
    const state$ = this.route.queryParamMap.pipe(map(params => {
      const filters = {...EMPTY_FILTERS};
      Object.keys(filters).forEach(key => filters[key] = params.get(key) ?? '');
      const requestedSize = Number(params.get('page_size') ?? 100);
      const pageSize = this.pageSizes.includes(requestedSize) ? requestedSize : 100;
      const requestedPage = Number(params.get('page') ?? 1);
      const page = Number.isInteger(requestedPage) ? Math.max(1, Math.min(10000 / pageSize, requestedPage)) : 1;
      return {
        ...filters, page, page_size: pageSize,
        sort: this.sortFields.some(field => field.value === params.get('sort')) ? params.get('sort') : 'task_id',
        direction: params.get('direction') === 'asc' ? 'asc' : 'desc',
      } as SearchState;
    }));

    state$.pipe(
      distinctUntilChanged((a, b) => JSON.stringify(a) === JSON.stringify(b)),
      switchMap(state => {
        this.state = state;
        this.syncGridSort();
        this.form.setValue(this.filters(state));
        this.tasksLoading = true;
        this.tasksError = '';
        this.result = null;
        return this.service.tasks(state).pipe(catchError(error => {
          this.tasksError = this.errorMessage(error);
          return of(null);
        }));
      }),
      takeUntilDestroyed(this.destroyRef),
    ).subscribe(result => {
      this.tasksLoading = false;
      this.result = result;
      if (result && this.state.page > this.pageCount) {
        this.navigate({page: this.pageCount}, true);
      }
    });

    state$.pipe(
      map(state => this.filters(state)),
      distinctUntilChanged((a, b) => JSON.stringify(a) === JSON.stringify(b)),
      switchMap(filters => {
        this.summaryLoading = true;
        this.summaryError = '';
        this.summary = null;
        return this.service.summary(filters).pipe(catchError(error => {
          this.summaryError = this.errorMessage(error);
          return of(null);
        }));
      }),
      takeUntilDestroyed(this.destroyRef),
    ).subscribe(summary => {
      this.summaryLoading = false;
      this.summary = summary;
    });
  }

  search(): void {
    const filters = this.form.getRawValue();
    Object.keys(filters).forEach(key => filters[key] = String(filters[key] ?? '').trim());
    this.navigate({...filters, page: 1});
  }

  clear(): void {
    this.navigate({...EMPTY_FILTERS, page: 1});
  }

  onGridReady(event: GridReadyEvent<DkbTask>): void {
    this.gridApi = event.api;
    this.syncGridSort();
  }

  onSortChanged(event: SortChangedEvent<DkbTask>): void {
    if (event.source !== 'uiColumnSorted') { return; }
    const sorted = event.api.getColumnState().find(column => column.sort);
    if (!sorted || !this.sortFields.some(field => field.value === sorted.colId)) { return; }
    if (sorted.colId !== this.state.sort || sorted.sort !== this.state.direction) {
      this.navigate({sort: sorted.colId, direction: sorted.sort, page: 1});
    }
  }

  private syncGridSort(): void {
    this.gridApi?.applyColumnState({
      state: [{colId: this.state.sort, sort: this.state.direction as 'asc' | 'desc'}],
      defaultState: {sort: null},
    });
  }

  navigate(changes: Partial<SearchState>, replaceUrl = false): void {
    void this.router.navigate([], {
      relativeTo: this.route, queryParams: {...this.state, ...changes}, replaceUrl,
    });
  }

  private filters(state: SearchState): SearchFilters {
    const filters = {...EMPTY_FILTERS};
    Object.keys(filters).forEach(key => filters[key] = state[key]);
    return filters;
  }

  private errorMessage(error: any): string {
    return error?.error?.detail ?? 'Could not load DKB search results. Please retry.';
  }

  private link(value: unknown, path: string): HTMLElement | string {
    if (value == null || !/^\d+$/.test(String(value))) { return ''; }
    const link = document.createElement('a');
    link.href = path + encodeURIComponent(String(value));
    link.textContent = String(value);
    return link;
  }

  private taskLinks(taskID: number): HTMLElement {
    const container = document.createElement('span');
    container.className = 'dkb-task-links';
    for (const [path, icon, title] of [
      ['/ng/task/', 'assignment', 'Open production task'],
      ['https://bigpanda.cern.ch/task/', 'open_in_new', 'Open BigPanDA task'],
    ]) {
      const link = this.link(taskID, path);
      if (typeof link === 'string') { continue; }
      link.textContent = '';
      link.title = title;
      link.setAttribute('aria-label', `${title} ${taskID}`);
      link.setAttribute('target', '_blank');
      link.setAttribute('rel', 'noopener noreferrer');
      const symbol = document.createElement('span');
      symbol.className = 'material-icons';
      symbol.setAttribute('aria-hidden', 'true');
      symbol.textContent = icon;
      link.appendChild(symbol);
      container.appendChild(link);
    }
    return container;
  }
}
