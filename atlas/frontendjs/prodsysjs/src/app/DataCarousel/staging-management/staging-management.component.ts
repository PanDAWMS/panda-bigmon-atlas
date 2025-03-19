import {Component, computed, inject, OnInit, ViewChild} from '@angular/core';
import {DataCarouselService, StagingRule} from "../data-carousel.service";
import {MatProgressSpinner} from "@angular/material/progress-spinner";
import {toObservable} from "@angular/core/rxjs-interop";
import {map, switchMap, take, takeUntil, tap} from "rxjs/operators";
import {BehaviorSubject, ReplaySubject, Subject} from "rxjs";
import {AgGridAngular} from "ag-grid-angular";
import {FilterChangedEvent, GridOptions, GridReadyEvent, RowNode, SelectionChangedEvent} from "ag-grid-community";
import {NgxMatSelectSearchModule} from "ngx-mat-select-search";
import {FormControl, ReactiveFormsModule} from "@angular/forms";
import {ActivatedRoute, Router} from '@angular/router';
import {SelectWithSearchComponent} from "../../common/select-with-search/select-with-search.component";
import {convertBytes} from "../../derivation-exclusion/dataset-size.pipe";
import {MatFormField, MatLabel} from "@angular/material/form-field";
import {MatInput} from "@angular/material/input";
import {MatButton} from "@angular/material/button";

@Component({
  selector: 'app-staging-management',
  imports: [
    MatProgressSpinner,
    AgGridAngular,
    NgxMatSelectSearchModule,
    ReactiveFormsModule,
    SelectWithSearchComponent,
    MatFormField,
    MatInput,
    MatLabel,
    MatButton
  ],
  templateUrl: './staging-management.component.html',
  styleUrl: './staging-management.component.css',
  standalone: true,
})
export class StagingManagementComponent implements OnInit {
  private dataCarouselService = inject(DataCarouselService);
  private router = inject(Router);
  private activatedRoute = inject(ActivatedRoute);

  @ViewChild('agGrid') rulesGrid!: AgGridAngular;
  stagingRules = computed(() => this.dataCarouselService.datasetStagingRulesResource.value() ?? []);
  filterParameters = toObservable(this.stagingRules).pipe(
    tap((rules) => {
      // Get all distinct destinations from the rules
      this.allDestinations = Array.from(new Set(rules.map(rule => rule.destination)));
      // Get all distinct sources from the rules
      this.allSources = Array.from(new Set(rules.map(rule => rule.source)));
      this.selectedDestinations.setValue(this.allDestinations);
      this.selectedSources.setValue(this.allSources);
      this.activatedRoute.queryParams.pipe(take(1)).subscribe(params => {
        const destParam = params.destination;
        if (destParam && Array.isArray(destParam)) {
          this.selectedDestinations.setValue(destParam);
        } else if (destParam && typeof destParam === 'string') {
          this.selectedDestinations.setValue([destParam]);
        } else {
          this.selectedDestinations.setValue(this.allDestinations);
        }
        const sourceParam = params.source;
        if (sourceParam && Array.isArray(sourceParam)) {
          this.selectedSources.setValue(sourceParam);
        } else if (sourceParam && typeof sourceParam === 'string') {
          this.selectedSources.setValue([sourceParam]);
        } else {
          this.selectedSources.setValue(this.allSources);
        }
        const generalParam = params.filter;
        if (generalParam) {
          this.generalFilter.setValue(generalParam);
        } else {
          this.generalFilter.setValue('');
        }
      });
    } ),
  );
  loading = this.dataCarouselService.datasetStagingRulesResource.isLoading;
  selectedDestinations = new FormControl([]);
  public allDestinations: string[] = [];
  public allSources: string[] = [];
  public selectedSources = new FormControl([]);
  public generalFilter = new FormControl('');
  public filterChanged$: Subject<number> = new Subject<number>();

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
    {field: 'destination', headerName: 'Destination'},
    {field: 'rse', headerName: 'Rule',
    // display only first and last 3 letters
    cellRenderer: params => {
      const rse = params.value;
      if (rse.length > 6) {
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

    this.selectedDestinations.valueChanges.subscribe(selected => {
      if (
        !selected ||
        selected.length === 0 ||
        (this.allDestinations && JSON.stringify(selected.sort()) === JSON.stringify(this.allDestinations.sort()))
      ) {
        // Remove parameter if ALL or none selected
        this.router.navigate([], {queryParams: {destination: null}, queryParamsHandling: 'merge'});
      } else {
        this.router.navigate([], {queryParams: {destination: selected}, queryParamsHandling: 'merge'});
      }
      this.filterChanged$.next(1);
    });
    this.selectedSources.valueChanges.subscribe( selected => {
        if (
          !selected ||
          selected.length === 0 ||
          (this.allSources && JSON.stringify(selected.sort()) === JSON.stringify(this.allSources.sort()))
        ) {
          // Remove parameter if ALL or none selected
          this.router.navigate([], {queryParams: {source: null}, queryParamsHandling: 'merge'});
        } else {
          this.router.navigate([], {queryParams: {source: selected}, queryParamsHandling: 'merge'});
        }
        this.filterChanged$.next(1);
      }
    );
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
    const selectedDestinations = this.selectedDestinations.value;
    const selectedSources = this.selectedSources.value;
    const generalFilter = this.generalFilter.value;

    const destinationFilter = selectedDestinations.length === 0 || selectedDestinations.includes(node.data.destination);
    const sourceFilter = selectedSources.length === 0 || selectedSources.includes(node.data.source);
    const generalFilterRegex = new RegExp(generalFilter, 'i');
    const generalFilterPass = node.data && (
      Boolean(node.data.dataset.match(generalFilterRegex)) ||
      Boolean(node.data.rse.match(generalFilterRegex)) ||
      Boolean(node.data.source.match(generalFilterRegex)) ||
      Boolean(node.data.destination.match(generalFilterRegex))
    );
    return destinationFilter && sourceFilter && generalFilterPass;

  }
  onSelectionChanged($event: SelectionChangedEvent<any>): void {
    console.log($event.api.getSelectedRows());
  }


  onGridReady(params: GridReadyEvent<any>) {
    params.api.autoSizeColumns(this.columnDefs.map( column => column.field), true);
  }

  adjustColumns(params: FilterChangedEvent<any>) {
        params.api.autoSizeColumns(this.columnDefs.map( column => column.field), true);
  }
}
