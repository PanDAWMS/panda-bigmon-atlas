import {Component, OnInit} from '@angular/core';
import {ProductionRequestBase} from "../production-request/production-request-models";
import {TasksManagementService} from "../tasks-management/tasks-management.service";
import {ActivatedRoute, Router} from "@angular/router";
import {catchError, debounceTime, filter, map, switchMap, tap} from "rxjs/operators";
import {DerivationExtensionService} from "./derivation-extension.service";
import {FormBuilder} from "@angular/forms";
import {BehaviorSubject, combineLatest, of} from "rxjs";
import {InputContainerItem} from "../group-analysis/create-analysis-request/create-analysis-request.component";
import {uniqByForEach} from "../common/tools";

@Component({
  selector: 'app-derivation-extension',
  templateUrl: './derivation-extension.component.html',
  styleUrls: ['./derivation-extension.component.css']
})
export class DerivationExtensionComponent implements OnInit {
  containersFormGroup = this.formBuilder.group({
    containerList: [''],
  });
  containersCurrentList: InputContainerItem[] = [];
  containersChecked$ = new BehaviorSubject<boolean>(false);
  containerListChanged$= new BehaviorSubject<string>('');
  containerChekedList: Map<string, number> = new Map<string, number>();
  error = '';
  patternLength = 0;

  separateInputContainerList$ = combineLatest([this.containerListChanged$, this.containersChecked$]).pipe(
    map(([input, _]) => input),
    debounceTime(300),
    // filter(input => input !== ''),
    map((input, _) => input.replace(/[\s,;]+/g, ',').split(',').filter((container) => container !== '')),
      // make a set to remove duplicates
      map((containerList) => uniqByForEach(containerList)),
  //   make InputContainerItem objects list
    map((containerList) => containerList.map((container) => {
      return {containerName: container, datasetNumber:
          this.containerChekedList.has(container) ? this.containerChekedList.get(container) : null} as InputContainerItem;
    } )), tap((containerList) => this.containersCurrentList = containerList),
  );
  public productionRequest$ = this.route.paramMap.pipe(switchMap((params) => {
      return this.tasksManagementService.getProductionRequest(params.get('requestID'));
    }
  ));

  public patternSteps$ = this.route.paramMap.pipe(switchMap((params) => {
    const slices = params.get('slices').split(',');
    return this.derivationExtensionService.getDerivationExtensionInfo(params.get('requestID'), slices.map(Number));
  }
), tap((patternSteps) => this.patternLength = patternSteps.length));
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

  constructor(private formBuilder: FormBuilder, private route: ActivatedRoute, private tasksManagementService: TasksManagementService,
              private derivationExtensionService: DerivationExtensionService, private router: Router) {
  }

  ngOnInit() {
  }

  extendRequest(): void {
    this.derivationExtensionService.extendDerivationRequest(this.route.snapshot.paramMap.get('requestID'),
      this.route.snapshot.paramMap.get('slices').split(',').map(Number),
      this.containersCurrentList.map((container) => container.containerName)).
    pipe(catchError((err, caught) => {
      this.error = err.toString() + err?.message;
      return of('');
    })).subscribe(
      (response) => {
        if (response === '') {
          return;
        }
        window.location.href = '/prodtask/inputlist_with_request/' + this.route.snapshot.paramMap.get('requestID');      }
    );
  }
}
