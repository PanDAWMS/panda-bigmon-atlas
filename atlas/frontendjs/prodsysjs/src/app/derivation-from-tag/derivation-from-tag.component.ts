import {
  AfterViewInit,
  Component,
  EventEmitter,
  Inject,
  Input,
  OnInit,
  Output,
  ViewChild,
  ViewEncapsulation
} from '@angular/core';
import {
  catchError, concatAll,
  debounceTime,
  distinctUntilChanged,
  filter,
  map, mergeAll,
  mergeMap,
  switchMap, take, takeUntil,
  tap,
  toArray
} from 'rxjs/operators';
import {ActivatedRoute} from '@angular/router';
import {DerivationFromTagService} from './derivation-from-tag.service';
import {BehaviorSubject, concat, merge, Observable, ReplaySubject, Subject} from "rxjs";
import {
  DerivationContainersCollection,
  DerivationContainersInput,
  DerivationDatasetInfo
} from "./derivation-request-models";
import {MatPaginator} from "@angular/material/paginator";
import {MatTableDataSource} from "@angular/material/table";
import {FormControl} from "@angular/forms";
import {MAT_DIALOG_DATA, MatDialog, MatDialogRef} from "@angular/material/dialog";
import {ProductionRequestBase} from "../production-request/production-request-models";
import {SelectionChangedEvent} from "ag-grid-community";
import {AgGridAngular} from "ag-grid-angular";

function get_output(output: string): string {
  return output.split('.')[output.split('.').length - 2]  ;
}

@Component({
  selector: 'app-derivation-from-tag',
  templateUrl: './derivation-from-tag.component.html',
  styleUrls: ['./derivation-from-tag.component.css'],
})
export class DerivationFromTagComponent implements OnInit, AfterViewInit  {
  @ViewChild('paginator') paginator: MatPaginator;
  @ViewChild('paginator2') paginator2: MatPaginator;
    @ViewChild('agGridOutputContainers') agGridOutputContainers!: AgGridAngular;

  @Input() outputOnly = false;
  @Output() selectedContainers: EventEmitter<string[]|[]> = new EventEmitter<string[]|[]>();
  private _onDestroy = new Subject<void>();

  constructor(public route: ActivatedRoute, public dialog: MatDialog, private derivationFromTagService: DerivationFromTagService) { }

  public currentAMITag = '';
  public loadingError?;
  public loadingData?: boolean;
  public containers$: BehaviorSubject <DerivationContainersInput[]> = new BehaviorSubject([]);
  public containersToCopy: string[] = [];
  public outputContainersToCopy: string[] = [];
  public outputContainersFilteredSelected: string[] = [];
  public currentRequests: ProductionRequestBase[] = [];
  public allOutputs: string[] = [];
  public allProjects: string[] = [];
  public filterChanged$: Subject<number> = new Subject<number>();
  searchTerms$ = new BehaviorSubject<string>('');
  public filteredContainers$ = merge(this.searchTerms$.pipe(debounceTime(300), distinctUntilChanged()),
    this.filterChanged$).pipe(switchMap(_ => this.containers$),
    map(containers =>
      containers.filter( container => (container.output_formats.filter(value => this.selectedOutputs.value.includes(value)).length !== 0)).
    filter( container => (container.requests_id.filter(value => this.selectedRequests.value.includes(value)).length !== 0)).
    filter( container => (container.projects.filter(value => this.selectedProjects.value.includes(value)).length !== 0))),
    map(containers => this.filterBroken ? containers.filter(container => !container.is_failed) : containers),
    map(containers => containers.filter( container => container.container.includes(this.containerNameFilterValue))),
    tap(containers => this.containersToCopy = containers.map(container => container.container))).pipe(
      map(containers => this.filterWrongName ? containers.filter(container => container.is_wrong_name) : containers),
      map(containers => this.filterRunningShow ? containers.filter(container => container.is_running) : containers),
      map(containers => this.filterBrokenShow ? containers.filter(container => container.is_failed) : containers),
    );
  public filteredOutputs$ = this.filteredContainers$.pipe(
    map(containers =>
      containers.map(container => container.output_containers).reduce((acc, val) => acc.concat(val), []).
        filter(output => this.selectedOutputs.value.includes(get_output(output)))),
          tap(outputs => this.outputContainersToCopy = outputs),
          map(output => output.map(x => ({container: x}))));

    public dataSource = new MatTableDataSource<DerivationContainersInput>([]);
    public dataSourceOutputs = new MatTableDataSource<string>([]);
  public derivationData$ = this.route.queryParamMap.pipe(tap(params => {
    if (params.has('amiTag')){
          this.currentAMITag = params.get('amiTag').toString();
          this.loadingData = true;
    }
    this.containers$.next([]);
    this.loadingError = null;
  }), filter(params => params.has('amiTag')),
    switchMap((params) =>   this.derivationFromTagService.getDerivationInputsByTag(params.get('amiTag').toString())),
    tap( receivedData => {
      this.loadingError = null;
      this.containers$.next(receivedData.containers);
      this.selectedOutputs.setValue(receivedData.format_outputs);
      this.allOutputs = receivedData.format_outputs;
      this.filteredSelectedOutputs$.next(this.allOutputs.slice());
      this.selectedRequests.setValue(receivedData.requests.map(productionRequest => productionRequest.reqid));
      this.currentRequests = receivedData.requests;
      this.currentRequestsIDs = receivedData.requests.map(productionRequest => productionRequest.reqid);
      this.filteredRequests$.next(this.currentRequests.slice());

      this.selectedProjects.setValue(receivedData.projects);
      this.allProjects = receivedData.projects;
      this.filteredProjects$.next(this.allProjects.slice());
      this.filterChanged$.next(1);
      this.loadingData = false;

    }),
     catchError( err => {
       this.loadingError = `Error data loading: ${err.error} `;
       this.loadingData = false;
       return `Error data loading ${err.toString()} `;
     })

    );
  selectedOutputs = new FormControl([]);
  selectedRequests = new FormControl([]);
  searchFilterRequests = new FormControl('');
  searchFilterOutputs = new FormControl('');
  searchFilterProjects = new FormControl('');
  selectedProjects = new FormControl([]);
  filteredRequests$: ReplaySubject<ProductionRequestBase[]> = new ReplaySubject<ProductionRequestBase[]>(1);
  filteredSelectedOutputs$: ReplaySubject<string[]> = new ReplaySubject<string[]>(1);
  filteredProjects$: ReplaySubject<string[]> = new ReplaySubject<string[]>(1);
  filterBroken = false;
  filterBrokenShow = false;
  filterRunningShow = false;
  filterWrongName = false;
  containerNameFilterValue = '';
  currentRequestsIDs: any[];
  outputAGColumns = [
    {
      field: 'container',
      headerName: 'Output',
      flex: 1,
    }];
  outputSelectionType: 'single' | 'multiple' | undefined = 'multiple';
  ngOnInit(): void {
    if ( ! this.outputOnly){
      this.outputSelectionType = undefined;
    }
    this.filteredContainers$.subscribe( data => {
      this.dataSource.data = data;
    });
    // this.filteredOutputs$.subscribe( data => {
    //   // this.dataSourceOutputs.data = data;
    // });
    this.searchFilterRequests.valueChanges.pipe(
      takeUntil(this._onDestroy)).subscribe(() => {
      this.filterRequestsMulti();
    } );
    this.searchFilterOutputs.valueChanges.pipe(
      takeUntil(this._onDestroy)).subscribe(() => {
      this.filterSimpleMulti(this.allOutputs, this.searchFilterOutputs, this.filteredSelectedOutputs$);
    } );
    this.searchFilterProjects.valueChanges.pipe(
      takeUntil(this._onDestroy)).subscribe(() => {
      this.filterSimpleMulti(this.allProjects, this.searchFilterProjects, this.filteredProjects$);
    } );
  }
  ngAfterViewInit(): void {
    this.dataSource.paginator = this.paginator;
    this.dataSourceOutputs.paginator = this.paginator2;
  }

  showContainerDetails(container: DerivationContainersInput): void {
     this.dialog.open(DialogContainerDetails, {
      data: container
    });
  }
  showSelectedContainers(data): void {
     this.dialog.open(DialogSelectedContainers, {
      data
    });
  }

  toggleSelectAll(selectAllValue: boolean, filteredValues$: ReplaySubject<any[]>, selectedValues: FormControl, allValues: any[]): void {
    filteredValues$.pipe(take(1), takeUntil(this._onDestroy))
      .subscribe(() => {
        if (selectAllValue) {
          selectedValues.patchValue([...allValues]);
        } else {
          selectedValues.patchValue([]);
        }
        this.filterChanged$.next(1);
      });
  }

  protected filterRequestsMulti(): void {
    if (!this.currentRequests) {
      return;
    }
    // get the search keyword
    let search: string = this.searchFilterRequests.value;
    if (!search) {
      this.filteredRequests$.next(this.currentRequests.slice());
      return;
    } else {
      search = search.toLowerCase();
    }
    // filter the banks
    this.filteredRequests$.next(
      this.currentRequests.filter(productionRequest =>
        (productionRequest.reqid.toString() + productionRequest.description).toLowerCase().indexOf(search) > -1)
    );
  }
  protected filterSimpleMulti(allValues: any[]|null, searchForm: FormControl, filteredValues$: ReplaySubject<string[]>): void {
    if (!allValues) {
      return;
    }
    // get the search keyword
    let search: string = searchForm.value;
    if (!search) {
      filteredValues$.next(allValues.slice());
      return;
    } else {
      search = search.toLowerCase();
    }
    // filter the banks
    filteredValues$.next(
      allValues.filter(value => value.toLowerCase().indexOf(search) > -1)
    );
  }

  onSelectionOutputChanged($event: SelectionChangedEvent<any>): void {
      this.outputContainersFilteredSelectedChanged();
   }

   outputContainersFilteredSelectedChanged(): void {
    this.outputContainersFilteredSelected = [];
    this.agGridOutputContainers.api.getSelectedNodes().forEach(node => {
      if (this.outputContainersToCopy.includes(node.data.container) &&
        !this.outputContainersFilteredSelected.includes(node.data.container)){
        this.outputContainersFilteredSelected.push(node.data.container);
      }
    }
    );
    this.selectedContainers.emit(this.outputContainersFilteredSelected);
   }
  toggleSelectFilteredOutputs(): void {
    if (this.outputContainersFilteredSelected.length === 0){
      this.agGridOutputContainers.api.selectAllFiltered();
    } else {
      this.agGridOutputContainers.api.deselectAll();
    }
    this.outputContainersFilteredSelectedChanged();
  }
}

@Component({
  selector: 'app-dialog-container-details',
  templateUrl: 'dialog-container-details.html',
  styleUrls: ['./derivation-from-tag.component.css']
})
export class DialogContainerDetails {
  constructor(
    public dialogRef: MatDialogRef<DialogContainerDetails>,
    @Inject(MAT_DIALOG_DATA) public container: DerivationContainersInput,
  ) {}
}

@Component({
  selector: 'app-dialog-selected-containers',
  templateUrl: 'dialog-selected-containers.html',
  styleUrls: ['./derivation-from-tag.component.css']
})
export class DialogSelectedContainers {
  constructor(
    public dialogRef: MatDialogRef<DialogSelectedContainers>,
    @Inject(MAT_DIALOG_DATA) public containers: string[],
  ) {}


}
