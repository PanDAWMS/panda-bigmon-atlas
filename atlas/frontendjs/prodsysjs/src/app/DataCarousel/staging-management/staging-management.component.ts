import {Component, computed, inject, input, OnInit, ViewChild} from '@angular/core';
import {DataCarouselService, StagingRule} from '../data-carousel.service';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {toObservable} from '@angular/core/rxjs-interop';
import {catchError, map, switchMap, take, takeUntil, tap} from 'rxjs/operators';
import {BehaviorSubject, of, ReplaySubject, Subject} from 'rxjs';
import {AgGridAngular} from 'ag-grid-angular';
import {FilterChangedEvent, GridOptions, GridReadyEvent, RowNode, SelectionChangedEvent} from 'ag-grid-community';
import {FormControl, FormsModule, ReactiveFormsModule, UntypedFormControl} from '@angular/forms';
import {ActivatedRoute, Router} from '@angular/router';
import {FilterBase, SelectWithSearchComponent} from '../../common/select-with-search/select-with-search.component';
import {convertBytes} from '../../derivation-exclusion/dataset-size.pipe';
import {MatFormField, MatLabel} from '@angular/material/form-field';
import {MatInput} from '@angular/material/input';
import {MatButton} from '@angular/material/button';
import {TasksManagementService} from '../../tasks-management/tasks-management.service';
import {ProductionTask} from '../../production-request/production-request-models';
import {ProductionTaskTableComponent} from '../../production-task-table/production-task-table.component';
import {MatSlideToggle} from "@angular/material/slide-toggle";
import {RuleActionComponent} from "../../rule-action/rule-action.component";
import {TaskStatsComponent} from "../../production-request/task-stats/task-stats.component";

@Component({
  selector: 'app-staging-management',
  imports: [
    MatProgressSpinner,
    AgGridAngular,
    ReactiveFormsModule,
    SelectWithSearchComponent,
    MatFormField,
    MatInput,
    MatLabel,
    MatButton,
    ProductionTaskTableComponent,
    MatSlideToggle,
    FormsModule,
    RuleActionComponent,
    TaskStatsComponent,
  ],
  templateUrl: './staging-management.component.html',
  styleUrl: './staging-management.component.css',
  standalone: true,
})
export class StagingManagementComponent implements OnInit {
  private dataCarouselService = inject(DataCarouselService);
  private router = inject(Router);
  private activatedRoute = inject(ActivatedRoute);
  private taskManagementService = inject(TasksManagementService);
  public filters = {
    source: new FilterBase('Source', [], 'source'),
    destination: new FilterBase('Destination', [], 'destination'),
    status: new FilterBase('Status', ['staging', 'queued'], 'status'),
    username: new FilterBase('Username', [], 'username'),
  };

  chosenTask = input<string>('');
  chosenDataset = input<string>('');

  @ViewChild('agGrid') rulesGrid!: AgGridAngular;
  stagingRules = computed(() => this.dataCarouselService.datasetStagingRulesResource.value()?.rules ?? []);
  fullRSEs = computed(() => this.dataCarouselService.datasetStagingRulesResource.value()?.fullRSEs ?? []);
  filterParameters = toObservable(this.stagingRules).pipe(
    tap((rules) => {
      this.filters.destination.updateValues(
        Array.from(
          new Set(
            rules
              .map(rule => rule.destination)
              .filter(destination => destination && destination.trim() !== '')
          )
        )
      );
      this.stuckErrors = {};
      this.stuckErrorOrder = [];
      for (const rule of rules) {
        if (rule.stuck) {
          this.stuckErrors[rule.stuck_error] = (this.stuckErrors[rule.stuck_error] || 0) + 1;
        }
      }
      this.stuckErrorOrder = ['total'].concat(
          Object.keys(this.stuckErrors).sort((a, b) => this.stuckErrors[b] - this.stuckErrors[a]));
      this.stuckErrorsControl.setValue(this.stuckErrorOrder);
      this.filters.source.updateValues(Array.from(new Set(rules.map(rule => rule.source))));
      this.filters.username.updateValues(Array.from(new Set(rules.reduce((acc, rule) => acc.concat(rule.owners), []))));
      this.activatedRoute.queryParams.pipe(take(1)).subscribe(params => {
        for (const filter of Object.values(this.filters)) {
          filter.takeValuesFromParams(params);
        }

        const generalParam = params.filter;
        if (generalParam) {
          this.generalFilter.setValue(generalParam);
        } else {
          this.generalFilter.setValue('');
        }
        const taskIDSParam = params.taskids;
        if (taskIDSParam) {
          this.taskids.setValue(taskIDSParam);
        } else {
          this.taskids.setValue('');
        }
        if (params.fullrses) {
          this.showOnlyFullRSEs.setValue(true);
        }
        if (params.stuck) {
          this.showOnlyStuckRules.setValue(true);
        }
      });
    } ),
  );
  ruleLoading = this.dataCarouselService.datasetStagingRulesResource.isLoading;
  public generalFilter = new FormControl('');
  public taskids = new FormControl('');
  public filterChanged$: Subject<number> = new Subject<number>();
  public selectedRules = [];
  public selectedDatasets = [];
  public selectedRulesString = '';
  public tasksToShow: ProductionTask[] = [];
  public taskLoading = false;
  public stuckErrors: {[status: string]: number} = {};
  public stuckErrorOrder: string[] = [];
  public stuckErrorsControl = new FormControl<string[]>([]);
  public showOnlyFullRSEs = new FormControl<boolean>(false);
  public showOnlyStuckRules = new FormControl<boolean>(false);
  public gridOptions: GridOptions = {
    isExternalFilterPresent: this.isExternalFilterPresent.bind(this),
    doesExternalFilterPass: this.doesExternalFilterPass.bind(this)
  };
  OS_ERROR_DASHBOARD_URL = `https://monit-grafana.cern.ch/d/e77b84d4-d854-4a18-b2c6-5fb67f648832/ddm-transfers-errors?from={time}&orgId=17&to=now&var-bin=1h&var-dataset_name={dataset_name}&var-dst_cloud=All&var-dst_country=All&var-dst_federation=All&var-dst_site=All&var-dst_tier=All&var-error_filter=&var-src_cloud=All&var-src_country=All&var-src_federation=All&var-src_site=All&var-src_tier=All&var-activity=Analysis%20Input&var-activity=Production%20Input&var-activity=Staging`;
  columnDefs = [
    {field: 'dataset', headerName: 'Dataset',
    // split dataset name by '.' and display only first and second to last fields
    cellRenderer: params => {
      let dataset = params.value;
      if (dataset.indexOf(':') !== -1) {
        dataset = dataset.split(':')[1];
      }
      const parts = dataset.split('.');
      if (parts.length > 2) {
        if (parts[0] === 'user' || parts[0] === 'group') {
          return `<a href="https://rucio-ui.cern.ch/did?scope=${parts[0]}.${parts[1]}&name=${dataset}">${parts[0]}|${parts[1]}</a>`;
        }
        let datasetFormat = '';
        if (parts[parts.length - 1] === 'RAW'){
          datasetFormat = parts[parts.length - 1];
        } else {
          datasetFormat = parts[parts.length - 2];
        }
        return `<a href="https://rucio-ui.cern.ch/did?scope=${parts[0]}&name=${dataset}">${parts[0]}|${datasetFormat}</a>`;
      }
      return dataset;
    },
      tooltipField: 'dataset',
    },
    {field: 'source', headerName: 'Source',
      cellRenderer: params => {
        if (params.data.empty_source){
          return `<b>${params.value}</b>`;
        }
        return params.value;
      }
    },
    {field: 'destination', headerName: 'Destination',
      cellRenderer: params => {
        if (this.fullRSEs().includes(params.value)){
          return `<span style="color: red">${params.value}</span>`;
        } else {
          return params.value;
        }
      }
    },
    {field: 'rse', headerName: 'Rule',
    // display only first and last 3 letters
    cellRenderer: params => {
      const rse = params.value;
      if (rse.length > 9) {
        if (params.data.stuck){
          return `<span style="color: red">!</span><a href="https://rucio-ui.cern.ch/rule?rule_id=${rse}">
                    ${rse.substring(0, 3)}...${rse.substring(rse.length - 3, rse.length)}</a>`;
        }
        return `<a href="https://rucio-ui.cern.ch/rule?rule_id=${rse}">${rse.substring(0, 3)}...${rse.substring(rse.length - 3, rse.length)}</a>`;
      }
      return rse;
    },
      tooltipField: 'rse',

    },
    {
      headerName: 'Err', field: 'dataset',
      maxWidth: 60,
      cellRenderer: params => {
        if (params.data.status === 'queued') {
          return '';
        }
        let dataset = params.value;
        if (dataset.indexOf(':') !== -1) {
          dataset = dataset.split(':')[1];
        }
        const starttime = params.data.start_time;
        const dashboardURL = this.OS_ERROR_DASHBOARD_URL.replace('{time}', starttime.toString()).replace('{dataset_name}', dataset);
        return `<a href="${dashboardURL}" target="_blank">(!)</a>`;
      }
    },
    {field: 'staged_files', headerName: 'Staged'},
    {field: 'total_files', headerName: 'Total'},
    {field: 'bytes', headerName: 'Size',
      cellRenderer: params => {
        return convertBytes(params.value);
      }},
    {field: 'update_time', headerName: 'Updated',
    cellRenderer: params => {
      return formatUnixTimestampUTC(params.value);
    }},

    {field: 'start_time', headerName: 'Started',
      cellRenderer: params => {
      return formatUnixTimestampUTC(params.value);
    }},


  ];

  constructor() {

        // Subscribe to selectedDestinations changes and update URL query parameter

    for (const filter of Object.values(this.filters)) {
      filter.selectedValues.valueChanges.subscribe((selected) => {
        this.filterChanged$.next(1);
      });
      }
    this.showOnlyFullRSEs.valueChanges.subscribe(filter => {
      if (filter) {
        this.router.navigate([], {queryParams: {fullrses: true}, queryParamsHandling: 'merge'});
      } else {
        this.router.navigate([], {queryParams: {fullrses: null}, queryParamsHandling: 'merge'});
      }
      this.filterChanged$.next(1);
    });
    this.showOnlyStuckRules.valueChanges.subscribe(filter => {
      if (filter) {
        this.router.navigate([], {queryParams: {stuck: true}, queryParamsHandling: 'merge'});
      } else {
        this.router.navigate([], {queryParams: {stuck: null}, queryParamsHandling: 'merge'});
      }
      this.filterChanged$.next(1);
    });
    this.generalFilter.valueChanges.subscribe( filter => {
      if (filter) {
        this.router.navigate([], {queryParams: {filter}, queryParamsHandling: 'merge'});
      } else {
        this.router.navigate([], {queryParams: {filter: null}, queryParamsHandling: 'merge'});
      }
      this.filterChanged$.next(1);
    });
    this.taskids.valueChanges.subscribe( filter => {
      if (filter) {
        this.router.navigate([], {queryParams: {taskids: filter}, queryParamsHandling: 'merge'});
      } else {
        this.router.navigate([], {queryParams: {taskids: null}, queryParamsHandling: 'merge'});
      }
      this.filterChanged$.next(1);
    });
    this.stuckErrorsControl.valueChanges.subscribe( _ => this.filterChanged$.next(1) );

  }

  ngOnInit(): void {
    if (this.chosenDataset()){
      this.dataCarouselService.selectedDCDataset.set(this.chosenDataset() ?? '');
    } else if (this.chosenTask()){
      this.dataCarouselService.selectedTask.set(this.chosenTask() ?? '');
    } else {
      this.dataCarouselService.getAllRules.set(true);
    }
    this.filterParameters.subscribe();
    this.filterChanged$.subscribe(
      () => {
        if (this.rulesGrid?.api) {
            this.rulesGrid.api.onFilterChanged();
        }
      }
    );
  }
  isExternalFilterPresent(): boolean {
    return true;
  }


  doesExternalFilterPass(node: RowNode<StagingRule>): boolean {
    const selectedDestinations = this.filters.destination.selectedValues.value;
    const selectedSources = this.filters.source.selectedValues.value;
    const generalFilter = this.generalFilter.value;
    const taskIDSFilter = this.taskids.value;
    const selectedStatus = this.filters.status.selectedValues.value;
    const selectedUsernames = this.filters.username.selectedValues.value;
    const fullRSEsFilter = ! this.showOnlyFullRSEs.value || this.fullRSEs().includes(node.data.destination);
    const stuckErrorFilter = this.stuckErrorsControl.value.length === 0 || ! node.data.stuck
      || this.stuckErrorsControl.value.includes(node.data.stuck_error);
    const stuckFilter = ! this.showOnlyStuckRules.value || node.data.stuck;
    const destinationFilter = selectedDestinations.length === 0 || selectedDestinations.includes(node.data.destination);
    const sourceFilter = selectedSources.length === 0 || selectedSources.includes(node.data.source);
    const statusFilter = selectedStatus.length === 0 || selectedStatus.includes(node.data.status);
    // get intersection of owners and selectedUsernames
    const usernameFilter = selectedUsernames.length === 0 || node.data.owners.some(owner => selectedUsernames.includes(owner));
    let taskIDS: string[] = [];
    if (taskIDSFilter){
    //   split string by all possible separators
        taskIDS = taskIDSFilter.split(/[\s,;]+/);
    }
    const taskIDFilter = taskIDS.length === 0 || taskIDS.some(taskID => node.data.tasks_ids.includes(Number(taskID)));

    let generalFilterPass = true;
    if (generalFilter) {
        const generalFilterRegex = new RegExp(generalFilter, 'i');
        generalFilterPass = node.data && (
          Boolean(node.data.dataset.match(generalFilterRegex)) ||
          Boolean(node.data.rse.match(generalFilterRegex))
        );
    }

    return stuckErrorFilter && stuckFilter && fullRSEsFilter && destinationFilter
      && sourceFilter && generalFilterPass && statusFilter && usernameFilter && taskIDFilter;

  }
  onSelectionChanged($event: SelectionChangedEvent<any>): void {
    this.filterOrSlectionChanged($event);
  }


  private filterOrSlectionChanged(params: SelectionChangedEvent<any>|FilterChangedEvent): void {
    this.selectedRules = [];
    this.rulesGrid.api.forEachNodeAfterFilter((selectedRule) => {
      if (selectedRule.isSelected()) {
        this.selectedRules.push(selectedRule.data);
      }
    });
    if (this.selectedRules.length === 0) {
      this.selectedRulesString = '';
      this.selectedDatasets = [];
      this.tasksToShow = [];
      return;
    }
    let stagedFiles = 0;
    let totalFiles = 0;
    let bytes = 0;
    const usernames = new Set<string>();
    let activeTasksNumber = 0;
    for (const rule of this.selectedRules) {
      stagedFiles += rule.staged_files;
      totalFiles += rule.total_files;
      bytes += rule.bytes;
      activeTasksNumber += rule.number_active_tasks;
      rule.owners.forEach(owner => usernames.add(owner));
    }
    let userNameSting = '';
    if (usernames.size < 5) {
      userNameSting = Array.from(usernames).join(', ');
    } else {
      userNameSting = `${usernames.size} users`;
    }
    const totalSize = convertBytes(bytes);
    this.selectedRulesString = `Selected ${this.selectedRules.length} rules with ${stagedFiles} staged files, ${totalFiles} total files, ${totalSize}, ${activeTasksNumber} active tasks and for ${userNameSting}`;
    this.selectedDatasets = this.selectedRules.map(rule => rule.dataset);
    this.tasksToShow = [];
  }

  onGridReady(params: GridReadyEvent<any>) {
    params.api.autoSizeColumns(this.columnDefs.map( column => column.field), true);
  }

  adjustColumns(params: FilterChangedEvent<any>) {
        params.api.autoSizeColumns(this.columnDefs.map( column => column.field), true);
        this.filterOrSlectionChanged(params);
  }

  showTasks() {
    const DCRules: {id: number, dc_type: string}[] = this.selectedRules.map(
      rule => {
        return {
          id: rule.id,
          dc_type: rule.dc_type
        };
      }
    );
    this.tasksToShow = [];
    this.taskLoading = true;
    this.taskManagementService.getTasksByDCRules(DCRules).pipe(
      catchError( err => {
        this.taskLoading = false;
        return of([] as ProductionTask[]);
      })
    ).subscribe(
      tasks => {
        this.tasksToShow = tasks;
        this.taskLoading = false;
      }
    );
  }

  selectFiltered() {
    this.rulesGrid.api.deselectAll();
    this.rulesGrid.api.selectAll('filtered');
  }

  clearSelection() {
    this.rulesGrid.api.deselectAll();
  }
}

function convertToUnixTimestamp(dateString: string): number {
  // Parse the date string from DD-MM-YYYY HH:MM:SS format
  const [datePart, timePart] = dateString.split(' ');
  const [day, month, year] = datePart.split('-');

  // Create a date object in YYYY-MM-DD format that JavaScript understands
  const dateObj = new Date(`${year}-${month}-${day}T${timePart}`);

  // Return the timestamp in milliseconds
  return dateObj.getTime();
}
function formatUnixTimestampUTC(timestamp: number): string {
  const date = new Date(timestamp);

  // Get date components in UTC
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');

  // Get time components in UTC
  const hours = String(date.getUTCHours()).padStart(2, '0');
  const minutes = String(date.getUTCMinutes()).padStart(2, '0');
  const seconds = String(date.getUTCSeconds()).padStart(2, '0');

  // Format as YYYY-MM-DD HH:MM:SS
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}
