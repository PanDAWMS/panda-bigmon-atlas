import {Component, computed, inject, OnInit, ViewChild} from '@angular/core';
import {DataCarouselService, StagingRule} from '../data-carousel.service';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {toObservable} from '@angular/core/rxjs-interop';
import {catchError, map, switchMap, take, takeUntil, tap} from 'rxjs/operators';
import {BehaviorSubject, of, ReplaySubject, Subject} from 'rxjs';
import {AgGridAngular} from 'ag-grid-angular';
import {FilterChangedEvent, GridOptions, GridReadyEvent, RowNode, SelectionChangedEvent} from 'ag-grid-community';
import {FormControl, FormsModule, ReactiveFormsModule} from '@angular/forms';
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
        if (params.fullrses) {
          this.showOnlyFullRSEs.setValue(true);
        }
      });
    } ),
  );
  ruleLoading = this.dataCarouselService.datasetStagingRulesResource.isLoading;
  public generalFilter = new FormControl('');
  public filterChanged$: Subject<number> = new Subject<number>();
  public selectedRules = [];
  public selectedRulesString = '';
  public tasksToShow: ProductionTask[] = [];
  public taskLoading = false;
  public showOnlyFullRSEs = new FormControl<boolean>(false);
  public gridOptions: GridOptions = {
    isExternalFilterPresent: this.isExternalFilterPresent.bind(this),
    doesExternalFilterPass: this.doesExternalFilterPass.bind(this)
  };
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
        return `<a href="https://rucio-ui.cern.ch/did?scope=${parts[0]}&name=${dataset}">${parts[0]}|${parts[parts.length - 2]}</a>`;
      }
      return dataset;
    },
      tooltipField: 'dataset',
    },
    {field: 'source', headerName: 'Source'},
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
        return `<a href="https://rucio-ui.cern.ch/rule?rule_id=${rse}">${rse.substring(0, 3)}...${rse.substring(rse.length - 3, rse.length)}</a>`;
      }
      return rse;
    },
      tooltipField: 'rse',

    },
    {field: 'staged_files', headerName: 'Staged'},
    {field: 'total_files', headerName: 'Total'},
    {field: 'bytes', headerName: 'Size',
      cellRenderer: params => {
        return convertBytes(params.value);
      }},
    {field: 'update_time', headerName: 'Updated'},
    {field: 'start_time', headerName: 'Started'},


  ];

  constructor() {
    this.filterParameters.subscribe();
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
    this.generalFilter.valueChanges.subscribe( filter => {
      if (filter) {
        this.router.navigate([], {queryParams: {filter}, queryParamsHandling: 'merge'});
      } else {
        this.router.navigate([], {queryParams: {filter: null}, queryParamsHandling: 'merge'});
      }
      this.filterChanged$.next(1);
    });

  }

  ngOnInit(): void {
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
    const selectedStatus = this.filters.status.selectedValues.value;
    const selectedUsernames = this.filters.username.selectedValues.value;
    const fullRSEsFilter = ! this.showOnlyFullRSEs.value || this.fullRSEs().includes(node.data.destination);
    const destinationFilter = selectedDestinations.length === 0 || selectedDestinations.includes(node.data.destination);
    const sourceFilter = selectedSources.length === 0 || selectedSources.includes(node.data.source);
    const statusFilter = selectedStatus.length === 0 || selectedStatus.includes(node.data.status);
    // get intersection of owners and selectedUsernames
    const usernameFilter = selectedUsernames.length === 0 || node.data.owners.some(owner => selectedUsernames.includes(owner));
    let generalFilterPass = true;
    if (generalFilter) {
        const generalFilterRegex = new RegExp(generalFilter, 'i');
        generalFilterPass = node.data && (
          Boolean(node.data.dataset.match(generalFilterRegex)) ||
          Boolean(node.data.rse.match(generalFilterRegex))
        );
    }

    return fullRSEsFilter && destinationFilter && sourceFilter && generalFilterPass && statusFilter && usernameFilter;

  }
  onSelectionChanged($event: SelectionChangedEvent<any>): void {
    this.selectedRules = $event.api.getSelectedRows();
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
    if (usernames.size < 5 ) {
      userNameSting = Array.from(usernames).join(', ');
    } else {
      userNameSting = `${usernames.size} users`;
    }
    const totalSize = convertBytes(bytes);
    this.selectedRulesString = `Selected ${this.selectedRules.length} rules with ${stagedFiles} staged files, ${totalFiles} total files, ${totalSize}, ${activeTasksNumber} active tasks and for ${userNameSting}`;

  }


  onGridReady(params: GridReadyEvent<any>) {
    params.api.autoSizeColumns(this.columnDefs.map( column => column.field), true);
  }

  adjustColumns(params: FilterChangedEvent<any>) {
        params.api.autoSizeColumns(this.columnDefs.map( column => column.field), true);
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
