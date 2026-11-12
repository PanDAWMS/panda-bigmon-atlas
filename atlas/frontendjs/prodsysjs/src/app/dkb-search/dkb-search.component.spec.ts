import {TestBed} from '@angular/core/testing';
import {ActivatedRoute, convertToParamMap, Router} from '@angular/router';
import {BehaviorSubject, of, Subject} from 'rxjs';
import {DkbSearchComponent} from './dkb-search.component';
import {DkbSearchService, TaskPage} from './dkb-search.service';
import {MatDialog} from '@angular/material/dialog';
import {DkbTaskDetailsComponent} from './dkb-task-details.component';
import {GridReadyEvent, SortChangedEvent} from 'ag-grid-community';

describe('DkbSearchComponent URL and server pagination', () => {
  let component: DkbSearchComponent;
  let params: BehaviorSubject<ReturnType<typeof convertToParamMap>>;
  let service: jasmine.SpyObj<DkbSearchService>;
  let router: jasmine.SpyObj<Router>;
  let dialog: jasmine.SpyObj<MatDialog>;

  beforeEach(() => {
    params = new BehaviorSubject(convertToParamMap({
      q: 'mc23*', step_name: 'Evgen', ctag: 'e8514', pr_id_min: '100',
      task_id_max: '999999', run_number_min: '410000', run_number_max: '419999',
      page: '2', page_size: '250', sort: 'total_events', direction: 'asc',
    }));
    service = jasmine.createSpyObj('DkbSearchService', ['tasks', 'summary']);
    service.tasks.and.returnValue(of({tasks: [], total: 12000, available: 10000,
      limit: 10000, page: 2, page_size: 250}));
    service.summary.and.returnValue(of({subcampaigns: [], total_tasks: 12000, total_events: 987654}));
    router = jasmine.createSpyObj('Router', ['navigate']);
    router.navigate.and.returnValue(Promise.resolve(true));
    dialog = jasmine.createSpyObj('MatDialog', ['open']);
    TestBed.configureTestingModule({providers: [
      {provide: ActivatedRoute, useValue: {queryParamMap: params}},
      {provide: DkbSearchService, useValue: service},
      {provide: Router, useValue: router},
      {provide: MatDialog, useValue: dialog},
    ]});
    component = TestBed.runInInjectionContext(() => new DkbSearchComponent());
    component.ngOnInit();
  });

  afterEach(() => TestBed.resetTestingModule());

  it('restores filters, ordering and pagination from a shared URL', () => {
    expect(component.form.getRawValue().q).toBe('mc23*');
    expect(component.form.getRawValue().task_id_max).toBe('999999');
    expect(service.tasks).toHaveBeenCalledWith(jasmine.objectContaining({
      page: 2, page_size: 250, sort: 'total_events', direction: 'asc', step_name: 'Evgen',
      run_number_min: '410000', run_number_max: '419999',
    }));
    expect(component.pageCount).toBe(40);
    expect(component.summary.total_tasks).toBe(12000);
  });

  it('fetches another server page without recomputing unchanged subcampaign totals', () => {
    params.next(convertToParamMap({
      ...component.state, page: '3',
    }));
    expect(service.tasks.calls.count()).toBe(2);
    expect(service.tasks.calls.mostRecent().args[0].page).toBe(3);
    expect(service.summary.calls.count()).toBe(1);
  });

  it('writes numeric inputs and filters to the URL and resets the page on search', () => {
    // Angular number inputs deliver numbers even though URL values are strings.
    component.form.patchValue({pr_id_min: 12345 as any, q: ' new query '});
    component.search();
    expect(router.navigate).toHaveBeenCalledWith([], jasmine.objectContaining({
      queryParams: jasmine.objectContaining({q: 'new query', pr_id_min: '12345', page: 1,
        page_size: 250, sort: 'total_events', direction: 'asc'}),
    }));
  });

  it('cancels an obsolete page request when the URL changes again', () => {
    const obsolete = new Subject<TaskPage>();
    service.tasks.and.returnValue(obsolete);
    params.next(convertToParamMap({...component.state, page: '3'}));
    service.tasks.and.returnValue(of({tasks: [], total: 12000, available: 10000,
      limit: 10000, page: 4, page_size: 250}));
    params.next(convertToParamMap({...component.state, page: '4'}));
    obsolete.next({tasks: [], total: 1, available: 1, limit: 10000, page: 3, page_size: 250});
    expect(component.result.page).toBe(4);
    expect(component.result.total).toBe(12000);
  });

  it('keeps malformed URL pagination within the 10,000 task window', () => {
    params.next(convertToParamMap({page: '999999', page_size: '1000', sort: 'unknown'}));
    expect(service.tasks.calls.mostRecent().args[0]).toEqual(jasmine.objectContaining({
      page: 10, page_size: 1000, sort: 'task_id', direction: 'desc',
    }));
  });

  it('opens DKB details when a task is chosen', () => {
    component.showTask(123456);
    expect(dialog.open).toHaveBeenCalledWith(DkbTaskDetailsComponent,
      jasmine.objectContaining({data: 123456}));
  });

  it('manages all matching tasks with every filter preserved', () => {
    component.result = {...component.result, total: 9999};
    component.manageTasks();
    expect(router.navigate).toHaveBeenCalledWith(['/tasks-by-dkb/dkb'], {
      queryParams: jasmine.objectContaining({q: 'mc23*', step_name: 'Evgen', ctag: 'e8514',
        pr_id_min: '100', task_id_max: '999999', run_number_min: '410000', run_number_max: '419999',
        search: 'mc23*', opensearch: '1'}),
    });
  });

  it('prevents management of zero, 10,000 or more matches and pending results', () => {
    for (const total of [0, 10000, 12000]) {
      component.result = {...component.result, total};
      component.manageTasks();
    }
    component.result = {...component.result, total: 100};
    component.tasksLoading = true;
    component.manageTasks();
    expect(router.navigate).not.toHaveBeenCalled();
  });

  it('sorts through the server with existing filters and resets pagination', () => {
    component.onSortChanged({source: 'uiColumnSorted', api: {
      getColumnState: () => [{colId: 'run_number', sort: 'desc'}],
    }} as SortChangedEvent);
    expect(router.navigate).toHaveBeenCalledWith([], jasmine.objectContaining({
      queryParams: jasmine.objectContaining({sort: 'run_number', direction: 'desc', page: 1,
        q: 'mc23*', run_number_min: '410000', page_size: 250}),
    }));
  });

  it('restores the header sort from the URL without triggering a navigation loop', () => {
    const applyColumnState = jasmine.createSpy('applyColumnState');
    component.onGridReady({api: {applyColumnState}} as unknown as GridReadyEvent);
    expect(applyColumnState).toHaveBeenCalledWith({
      state: [{colId: 'total_events', sort: 'asc'}], defaultState: {sort: null},
    });
    params.next(convertToParamMap({...component.state, sort: 'taskname', direction: 'desc'}));
    expect(applyColumnState).toHaveBeenCalledWith({
      state: [{colId: 'taskname', sort: 'desc'}], defaultState: {sort: null},
    });
    component.onSortChanged({source: 'api'} as SortChangedEvent);
    expect(router.navigate).not.toHaveBeenCalled();
    expect(service.tasks.calls.mostRecent().args[0].sort).toBe('taskname');
    expect(service.summary.calls.count()).toBe(1);
  });
});
