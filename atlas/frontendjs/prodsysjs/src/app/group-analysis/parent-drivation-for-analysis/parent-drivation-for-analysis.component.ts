import {Component, EventEmitter, Output, ViewChild} from '@angular/core';
import {catchError, debounceTime, distinctUntilChanged, filter, map, switchMap, tap} from "rxjs/operators";
import {DerivationFromTagService} from "../../derivation-from-tag/derivation-from-tag.service";
import {ActivatedRoute} from "@angular/router";
import {AnalysisTasksService} from "../analysis-tasks.service";
import {BehaviorSubject, merge, Subject} from "rxjs";
import {SelectionChangedEvent} from "ag-grid-community";
import {FormControl} from "@angular/forms";
import {AgGridAngular} from "ag-grid-angular";

@Component({
  selector: 'app-parent-drivation-for-analysis',
  templateUrl: './parent-drivation-for-analysis.component.html',
  styleUrls: ['./parent-drivation-for-analysis.component.css']
})
export class ParentDrivationForAnalysisComponent {

  @ViewChild('agGridInputSlices') agGridInputSlices!: AgGridAngular;
  @Output() selectedSlices: EventEmitter<{slice: number, outputFormat: string, requestID: string, container: string}[]|[]>
    = new EventEmitter<{slice: number, outputFormat: string, requestID: string, container: string}[]|[]>();

  public currentParentRequest: string;
  public loadingData = false;
  public loadingError: string|null = null;
  public slices$: BehaviorSubject<{slice: number, outputFormat: string, container: string}[]> = new BehaviorSubject([]);
  public outputFormats: string[] = [];
  public inputSlicesFilteredSelected: {slice: number, outputFormat: string, container: string}[] = [];
  public  searchTerms$ = new BehaviorSubject<string>('');
  public filterChanged$: Subject<number> = new Subject<number>();
  public searchTermFilterValue = '';
  public filteredInputSlices$ = merge(this.searchTerms$.pipe(debounceTime(300), distinctUntilChanged()),
    this.filterChanged$).pipe(switchMap(_ => this.slices$),
    map(slices =>
      slices.filter( slice => slice.container.toLowerCase().includes(this.searchTermFilterValue.toLowerCase())).
      filter( slice => this.outputFormatsForm.value === '' || slice.outputFormat === this.outputFormatsForm.value)),
    tap( slices => {
      this.inputSlicesFilteredSelectedChanged();
    }
    ));
  public parentRequestData$ = this.route.queryParamMap.pipe(tap(params => {
    if (params.has('parentRequest')){
          this.currentParentRequest = params.get('parentRequest').toString();
          this.loadingData = true;
    }
    this.slices$.next([]);
    this.loadingError = null;
  }), filter(params => params.has('parentRequest')),
    switchMap((params) =>   this.analysisTasksService.getParentDerivation(params.get('parentRequest').toString())),
    tap( receivedData => {
      this.slices$.next(receivedData.slices);
      this.searchTerms$.next('');
      this.outputFormats = receivedData.outputFormats;
      this.outputFormatsForm.setValue(this.outputFormats[0]);
      this.filterChanged$.next(0);
      this.loadingData = false;

    }),
     catchError( err => {
       this.loadingError = `Error data loading: ${err.error} `;
       this.loadingData = false;
       return `Error data loading ${err.toString()} `;
     })

    );
  inputAGColumns = [
    {field: 'slice', headerName: 'Slice', width: 30},
    {field: 'container', headerName: 'Container', flex: 1},
  ];
  outputFormatsForm = new FormControl('');

  constructor(private route: ActivatedRoute, private analysisTasksService: AnalysisTasksService) {
  }


   onSelectionInputSlicesChanged($event: SelectionChangedEvent<any>): void {
      this.inputSlicesFilteredSelectedChanged();
   }

   inputSlicesFilteredSelectedChanged(): void {
    this.inputSlicesFilteredSelected = [];
    if (this.agGridInputSlices) {
      this.agGridInputSlices.api.getSelectedNodes().forEach(node => {
        if (!this.inputSlicesFilteredSelected.includes(node.data)){
          this.inputSlicesFilteredSelected.push(node.data);
        }
      }
      );
      this.selectedSlices.emit(this.inputSlicesFilteredSelected.map(slice => {
        return {slice: slice.slice, outputFormat: slice.outputFormat, requestID: this.currentParentRequest, container: slice.container};
      }));
    }

   }
  toggleSelectFilteredOutputs(): void {
    if (this.inputSlicesFilteredSelected.length === 0){
      this.agGridInputSlices.api.selectAllFiltered();
    } else {
      this.agGridInputSlices.api.deselectAll();
    }
    this.inputSlicesFilteredSelectedChanged();
  }
}
