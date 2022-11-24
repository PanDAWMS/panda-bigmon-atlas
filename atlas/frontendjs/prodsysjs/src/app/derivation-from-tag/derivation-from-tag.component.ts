import {AfterViewInit, Component, Inject, OnInit, ViewChild} from '@angular/core';
import {
  catchError,
  debounceTime,
  distinctUntilChanged,
  filter,
  map,
  mergeMap,
  switchMap,
  tap,
  toArray
} from 'rxjs/operators';
import {ActivatedRoute} from '@angular/router';
import {DerivationFromTagService} from './derivation-from-tag.service';
import {BehaviorSubject, merge, Observable, Subject} from "rxjs";
import {
  DerivationContainersCollection,
  DerivationContainersInput,
  DerivationDatasetInfo
} from "./derivation-request-models";
import {MatPaginator} from "@angular/material/paginator";
import {MatTableDataSource} from "@angular/material/table";
import {FormControl} from "@angular/forms";
import {MAT_DIALOG_DATA, MatDialog, MatDialogRef} from "@angular/material/dialog";

@Component({
  selector: 'app-derivation-from-tag',
  templateUrl: './derivation-from-tag.component.html',
  styleUrls: ['./derivation-from-tag.component.css']
})
export class DerivationFromTagComponent implements OnInit, AfterViewInit  {
  @ViewChild(MatPaginator) paginator: MatPaginator;

  constructor(public route: ActivatedRoute, public dialog: MatDialog, private derivationFromTagService: DerivationFromTagService) { }

  public currentAMITag = '';
  public loadingError?;
  public loadingData?: boolean;
  public containers$: BehaviorSubject <DerivationContainersInput[]> = new BehaviorSubject([]);
  public containersToCopy: string[] = [];
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
  public dataSource = new MatTableDataSource<DerivationContainersInput>([]);
  public derivationData$ = this.route.paramMap.pipe(tap(params => {
    this.currentAMITag = params.get('amiTag').toString();
    this.containers$.next([]);
    this.loadingData = true;
    this.loadingError = null;
  }),
    switchMap((params) =>   this.derivationFromTagService.getDerivationInputsByTag(params.get('amiTag').toString())),
    tap( receivedData => {
      this.loadingError = null;
      this.containers$.next(receivedData.containers);
      this.selectedOutputs.setValue(receivedData.format_outputs);
      this.selectedRequests.setValue(receivedData.requests.map(productionRequest => productionRequest.reqid));
      this.selectedProjects.setValue(receivedData.projects);
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
  selectedProjects = new FormControl([]);
  filterBroken = false;
  filterBrokenShow = false;
  filterRunningShow = false;
  filterWrongName = false;
  containerNameFilterValue = '';
  ngOnInit(): void {

    this.filteredContainers$.subscribe( data => {
      this.dataSource.data = data;
    });

  }
  ngAfterViewInit(): void {
    this.dataSource.paginator = this.paginator;
  }

  showContainerDetails(container: DerivationContainersInput): void {
     this.dialog.open(DialogContainerDetails, {
      data: container
    });
  }
  showSelectedContainers(): void {
     this.dialog.open(DialogSelectedContainers, {
      data: this.containersToCopy
    });
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
