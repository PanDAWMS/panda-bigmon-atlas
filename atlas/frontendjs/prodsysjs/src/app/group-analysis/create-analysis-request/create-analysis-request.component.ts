import { Component, OnInit } from '@angular/core';
import {BehaviorSubject, combineLatest, merge, Subject} from "rxjs";
import {catchError, debounceTime, filter, map, tap} from "rxjs/operators";
import {FormBuilder, Validators} from "@angular/forms";
import {GridReadyEvent} from "ag-grid-community";
import {AnalysisTasksService} from "../analysis-tasks.service";
import {TemplateBase} from "../analysis-task-model";
import {editState, PatternChanges} from "../pattern-edit/pattern-edit.component";
import {Router} from "@angular/router";


interface InputContainerItem {
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
    requestDescriptionCtrl: ['', Validators.required],
  });
  containersCurrentList: InputContainerItem[] = [];
  chosenTemplate: TemplateBase|null = null;
  editMode: editState = 'view';
  submissionError = '';
  containerListChanged$= new BehaviorSubject<string>('');
  containersChecked$ = new BehaviorSubject<boolean>(false);
  patterns$ = this.analysisTasksService.getAllActiveTemplates().pipe(
    tap((patterns) => console.log(patterns)));
  // debunk input, wait 300ms, filter empty string, replace all possible separators with commas, split on commas, remove empty strings
  // change when containersChecked$ emits
  separateInputContainerList$ = combineLatest([this.containerListChanged$, this.containersChecked$]).pipe(
    map(([input, _]) => input),
    debounceTime(300),
    filter(input => input !== ''),
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
  checkContainers(): void {
    for (const container of this.containersCurrentList){
      this.containerChekedList.set(container.containerName, 1);
    }
    this.containersChecked$.next(true);
    this.containersChecked = true;
  }
  constructor(private formBuilder: FormBuilder, private analysisTasksService: AnalysisTasksService,
              private router: Router) { }

  ngOnInit(): void {
  }

  onGridReady($event: GridReadyEvent<any>) {

  }

  changeTaskTemplate($event): void {
    console.log(this.templateChoiceFormGroup.get('patternCntrl').value);
    if (this.templateChoiceFormGroup.get('patternCntrl').value) {
      this.chosenTemplate = null;
      // deep copy
      this.chosenTemplate = JSON.parse(JSON.stringify(this.templateChoiceFormGroup.get('patternCntrl')?.value)) as TemplateBase;
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
    this.analysisTasksService.createAnalysisRequest(this.requestDescriptionFormGroup.get('requestDescriptionCtrl').value,
      this.chosenTemplate, this.containersCurrentList.map(container => container.containerName)).pipe(
      catchError( err =>  { this.submissionError = err.error;
                            return null; })).
    subscribe((requestID) => { if (requestID !== null) {
      this.router.navigate(['analysis-request', requestID]);
    } });
  }
}
