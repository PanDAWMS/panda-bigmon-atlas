import { Component, OnInit } from '@angular/core';
import {BehaviorSubject, combineLatest, merge, Subject} from "rxjs";
import {catchError, debounceTime, filter, map, tap} from "rxjs/operators";
import {FormBuilder, Validators} from "@angular/forms";
import {GridReadyEvent} from "ag-grid-community";
import {AnalysisTasksService} from "../analysis-tasks.service";
import {TemplateBase} from "../analysis-task-model";
import {editState, PatternChanges} from "../pattern-edit/pattern-edit.component";
import {ActivatedRoute, Router} from "@angular/router";


export interface InputContainerItem {
  containerName: string;
  datasetNumber: number|null;
}
@Component({
  selector: 'app-create-analysis-request',
  templateUrl: './create-analysis-request.component.html',
  styleUrls: ['./create-analysis-request.component.css']
})
export class CreateAnalysisRequestComponent implements OnInit {
  // Map of container names to dataset numbers
  containerChekedList: Map<string, number> = new Map<string, number>();
  containersFormGroup = this.formBuilder.group({
    containerList: [''],
  });
  templateChoiceFormGroup= this.formBuilder.group({
    patternCntrl: [null],
  });
  requestDescriptionFormGroup = this.formBuilder.group({
    requestDescriptionCtrl: [''],
    requestExtentionCtrl: [''],
    requestScopeCtrl: [''],
  });
  patternTag: string|null = null;
  containersCurrentList: InputContainerItem[] = [];
  chosenTemplate: TemplateBase|null = null;
  editMode: editState = 'view';
  extendedRequest: number|null = null;
  submissionError = '';
  originalScopes: string[] = [];
  scopeList: string[] = [];
  creatingRequest = false;
  inputSlices: {slice: number; outputFormat: string; requestID: string; container: string}[] = [];
  selectedTabDescription = 0;
  containerListChanged$= new BehaviorSubject<string>('');
  containersChecked$ = new BehaviorSubject<boolean>(false);
  patterns$ = combineLatest( [this.analysisTasksService.getAllActiveTemplates(), this.route.paramMap, this.analysisTasksService.getAnalysisScopes()]).pipe(
     map(([patterns, params, scopes]) => {
       if (params.has('tag')) {
         this.patternTag = params.get('tag');
       }
       this.originalScopes = scopes;
       if (this.patternTag !== null && patterns.length > 0) {
        const requestedPattern = patterns.find((pattern) => pattern.tag === this.patternTag);
        if (requestedPattern) {
                  this.templateChoiceFormGroup.get('patternCntrl').setValue(requestedPattern);
                  this.chosenTemplate = requestedPattern;
        }
        this.changeScopeList();

      }
       return patterns;
    }));
  // debunk input, wait 300ms, filter empty string, replace all possible separators with commas, split on commas, remove empty strings
  // change when containersChecked$ emits
  separateInputContainerList$ = combineLatest([this.containerListChanged$, this.containersChecked$]).pipe(
    map(([input, _]) => input),
    debounceTime(300),
    // filter(input => input !== ''),
    map((input, _) => input.replace(/[\s,;]+/g, ',').split(',').filter((container) => container !== ''),
      // make a set to remove duplicates
      (containerList) => Array.from(new Set(containerList))),
  //   make InputContainerItem objects list
    map((containerList) => containerList.map((container) => {
      return {containerName: container, datasetNumber:
          this.containerChekedList.has(container) ? this.containerChekedList.get(container) : null} as InputContainerItem;
    } )), tap((containerList) => this.containersCurrentList = containerList),
  );
  // single column with container names
  columnDefs = [
    {headerName: 'Container Name', field: 'containerName', sortable: true, filter: true, resizable: true, flex: 1},
    {headerName: 'Dataset Number', field: 'datasetNumber', sortable: true, filter: true, resizable: true, flex: 1,
    // render "NotChecked" if null and "Empty" if 0
      valueFormatter: (params) => {
        if (params.value === null) {
          return 'NotChecked';
        }
        if (params.value === 0) {
          return 'Empty';
        }
        return params.value;
      } },
  ];
  containersChecked = false;
  selectedTabInputContainers = 0;
  checkContainers(): void {
    for (const container of this.containersCurrentList){
      this.containerChekedList.set(container.containerName, 1);
    }
    this.containersChecked$.next(true);
    this.containersChecked = true;
  }
  constructor(private formBuilder: FormBuilder, private analysisTasksService: AnalysisTasksService,
              private router: Router, private route: ActivatedRoute) { }

  ngOnInit(): void {
     this.route.queryParamMap.subscribe((queryParams) => {
        if (queryParams.has('requestID')){
          console.log(queryParams.get('requestID'));
          this.requestDescriptionFormGroup.get('requestExtentionCtrl').setValue( queryParams.get('requestID'));
          this.selectedTabDescription = 1;        }
        if (queryParams.has('amiTag')){
          this.selectedTabInputContainers = 1;
        } else if (queryParams.has('parentRequest')){
          this.selectedTabInputContainers = 2;
        }
     }
      );
  }

  onGridReady($event: GridReadyEvent<any>) {

  }
  changeScopeList(): void {
    this.scopeList = this.originalScopes;
    if (this.chosenTemplate.task_parameters.workingGroup) {
      const newScope = this.scopeList.filter((scope) => scope.includes(this.chosenTemplate.task_parameters.workingGroup))[0];
      this.scopeList = [newScope].concat(this.scopeList.filter((scope) => scope !== newScope));
    }
  }
  changeTaskTemplate($event): void {
    console.log(this.templateChoiceFormGroup.get('patternCntrl').value);
    if (this.templateChoiceFormGroup.get('patternCntrl').value) {
      this.chosenTemplate = null;
      // deep copy
      this.chosenTemplate = JSON.parse(JSON.stringify(this.templateChoiceFormGroup.get('patternCntrl')?.value)) as TemplateBase;
      this.changeScopeList();
    }
  }

    changeTemplate(data: PatternChanges): void {
    for (const key of data.removedFields){
      delete this.chosenTemplate.task_parameters[key];
    }
    for (const key of Object.keys(data.changes)){
      this.chosenTemplate.task_parameters[key] = data.changes[key];
    }
  }

  createRequest(): void {
    this.creatingRequest = true;
    if (this.inputSlices.length === 0) {
      this.analysisTasksService.createAnalysisRequest(this.requestDescriptionFormGroup.get('requestDescriptionCtrl').value,
        this.requestDescriptionFormGroup.get('requestScopeCtrl').value,
        this.requestDescriptionFormGroup.get('requestExtentionCtrl').value,
        this.chosenTemplate, this.containersCurrentList.map(container => container.containerName)).pipe(
        catchError(err => {
          this.submissionError = err.error;
          this.creatingRequest = false;
          return null;
        })).subscribe((requestID) => {
        if (requestID !== null) {
          this.creatingRequest = false;
          this.router.navigate(['analysis-request', requestID]);
        }
      });
    } else {
      this.analysisTasksService.createAnalysisRequestFromSlices(this.requestDescriptionFormGroup.get('requestDescriptionCtrl').value,
        this.requestDescriptionFormGroup.get('requestScopeCtrl').value,
        this.requestDescriptionFormGroup.get('requestExtentionCtrl').value,
        this.chosenTemplate, this.inputSlices).pipe(
        catchError(err => {
          this.submissionError = err.error;
          this.creatingRequest = false;
          return null;
        })).subscribe((requestID) => {
        if (requestID !== null) {
          this.creatingRequest = false;
          this.router.navigate(['analysis-request', requestID]);
        }
      });
    }
  }

  containersChanges(selectedContainers: string[] | []): void {
    if(selectedContainers.length === 0){
      this.containerListChanged$.next('');
    }
    this.containerListChanged$.next(selectedContainers.join(','));
  }

  inputSlicesChanges(slices: {slice: number; outputFormat: string; requestID: string; container: string}[] | []) {
    if (slices.length === 0){
      this.inputSlices = [];
      this.containerListChanged$.next('');
    } else {
      this.inputSlices = slices;
      this.containerListChanged$.next(slices.map(slice => `Request:${slice.requestID}-Slice:${slice.slice}-${slice.container}`).join(','));
    }
  }
}
