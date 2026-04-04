import {Component, computed, effect, inject, input, OnInit, Signal} from '@angular/core';
import {
  DataCarouselService,
  DatasetDeletedResponse,
  DatasetExistsResponse
} from "../../DataCarousel/data-carousel.service";
import {AsyncPipe, DatePipe, DecimalPipe, JsonPipe} from "@angular/common";
import {MatProgressSpinner} from "@angular/material/progress-spinner";
import {HttpErrorResponse} from "@angular/common/http";
import {setErrorMessage} from "../../dsid-info/dsid-info.service";
import {RucioURLPipe} from "../../derivation-exclusion/rucio-url.pipe";
import {DatasetSizePipe} from "../../derivation-exclusion/dataset-size.pipe";
import {RouterLink} from "@angular/router";
import {ProductionTask, RucioReplica, RucioRule} from "../production-request-models";
import {
  MatCell, MatCellDef,
  MatColumnDef,
  MatHeaderCell,
  MatHeaderCellDef,
  MatHeaderRow, MatHeaderRowDef,
  MatRow, MatRowDef,
  MatTable, MatTableDataSource
} from "@angular/material/table";
import {MatSort} from "@angular/material/sort";
import {Observable, of} from "rxjs";
import {TaskActionLog, TaskService} from "../../production-task/task-service.service";
import {toObservable} from "@angular/core/rxjs-interop";
import {RuleActionComponent} from "../../rule-action/rule-action.component";
import {MatCard, MatCardHeader, MatCardTitle} from "@angular/material/card";
import {TasksManagementService} from "../../tasks-management/tasks-management.service";
import {catchError} from "rxjs/operators";
import {ProductionTaskTableComponent} from "../../production-task-table/production-task-table.component";


interface ReplicaWithRule {
  rule: RucioRule|undefined;
  replica: RucioReplica| undefined;
}

@Component({
  selector: 'app-rucio-did',
  imports: [
    MatProgressSpinner,
    RucioURLPipe,
    DatasetSizePipe,
    DatePipe,
    RouterLink,
    MatTable,
    MatHeaderCell,
    MatCell,
    MatHeaderRow,
    MatRow,
    MatSort,
    MatColumnDef,
    MatHeaderCellDef,
    MatCellDef,
    MatHeaderRowDef,
    MatRowDef,
    RuleActionComponent,
    DecimalPipe,
    MatCard,
    MatCardHeader,
    MatCardTitle,
    AsyncPipe,
    ProductionTaskTableComponent
  ],
  templateUrl: './rucio-did.component.html',
  styleUrl: './rucio-did.component.css'
})
export class RucioDIDComponent implements OnInit {
    private dataCarouselService = inject(DataCarouselService);
    private taskService = inject(TaskService);
    private taskManagementService = inject(TasksManagementService);
    public tasksToShow: ProductionTask[] = [];
    public taskLoading = false;
    public actionLog$: Observable<TaskActionLog[]>;
    showStaging = input<boolean>(true);
    datasetName = input<string>();
    datasetName$ = toObservable(this.datasetName);
    datasetRecreated = computed(() => this.dataCarouselService.datasetInfoResource.value()?.recreated_dataset);
    datasetExists = computed(() => this.dataCarouselService.datasetInfoResource.value()?.dataset_exists );
    datasetInfo: Signal<DatasetExistsResponse|undefined> = computed(() => {
      if (this.datasetExists()){
        return this.dataCarouselService.datasetInfoResource.value().dataset_knowledge as DatasetExistsResponse;
      }else {
        return undefined;
      }
    });
    datasetDeletedInfo: Signal<DatasetDeletedResponse|undefined> = computed(() => {
      if (!this.datasetExists()){
        return this.dataCarouselService.datasetInfoResource.value().dataset_knowledge as DatasetDeletedResponse;
      }else {
        return undefined;
      }
    });
    stuckStaging = computed(() => {
      if (this.datasetExists()){
        if (this.datasetInfo().staging_dataset){
          const staging_dataset = this.datasetInfo().staging_dataset;
          const eightDaysAgoInMs = Date.now() - (8 * 24 * 60 * 60 * 1000);

          if (staging_dataset.status.toLowerCase() === 'staging' &&
            staging_dataset.start_time && staging_dataset.start_time < eightDaysAgoInMs){
            return staging_dataset.dataset;
          }
        }
      }
      return '';
    });
    stuckStaging$ = toObservable(this.stuckStaging);
    stuckFiles = computed(() => this.dataCarouselService.stuckFilesResource.value());
    stuckFilesError = computed(() => this.dataCarouselService.stuckFilesResource.error() as HttpErrorResponse | null);
    stuckFilesMessage = computed(() => {
      if (this.stuckFilesError()) {
        return setErrorMessage(this.stuckFilesError());
      }else {
        return null;
      }
    });
    stuckFilesLoading = this.dataCarouselService.stuckFilesResource.isLoading;
    stuckReasons = computed(() => {
      if (this.datasetExists()){
        const reasons = new Set<string>();
        for (const rule of this.datasetInfo().rules){
           if (rule.state === 'STUCK' || rule.state === 'SUSPENDED'){
             reasons.add(rule.error);
           }
        }
        return Array.from(reasons);
      }
      return [];
    });
    displayedColumns: string[] = [
    'ruleRSEExpression',
    'files',
    'ruleStatus',
    'ruleActivity',
    'ruleAccount',
    'sourceRSE',
    'expiresAt'
  ];
    dataSource = new MatTableDataSource<ReplicaWithRule>();
    rulesWithReplicas = computed(() => {
      const rulesWithReplicas: ReplicaWithRule[] = [];
      if (this.datasetExists()){
        const datasetResponse = this.dataCarouselService.datasetInfoResource.value().dataset_knowledge as DatasetExistsResponse;
        const replicasMap = new Map<string, RucioReplica>();
        const bigEnoughReplicas = new Map<string, RucioReplica>();
        for (const replica of datasetResponse.replicas){
          replicasMap.set(replica.rse, replica);
          if ((replica.available_length / replica.length) > 0.95) {
            bigEnoughReplicas.set(replica.rse, replica);
          }
        }
        for (const rule of datasetResponse.rules){
           rulesWithReplicas.push({
            rule,
            replica: replicasMap.get(rule.rse_expression)
           });
           bigEnoughReplicas.delete(rule.rse_expression);
        }
        for (const replica of bigEnoughReplicas.values()){
          rulesWithReplicas.push({
            rule: undefined,
            replica
          });
        }
      }
      return rulesWithReplicas;

    });
    isLoading = this.dataCarouselService.datasetInfoResource.isLoading;
    error = computed(() => this.dataCarouselService.datasetInfoResource.error() as HttpErrorResponse | null);
    errorMessage = computed(() => {
      if (this.error()) {
        return setErrorMessage(this.error());
      }else {
        return null;
      }
    });
  Object: any = Object;
    constructor() {
      effect(() => {
        this.dataSource.data = this.rulesWithReplicas();
    });
    }

  ngOnInit(): void {
        // this.dataCarouselService.datasetName.set(this.datasetName());
        this.datasetName$.subscribe(datasetName => {
          if (datasetName){
            this.dataCarouselService.datasetName.set(datasetName);
            this.actionLog$ = this.taskService.getRuleActionLogs(this.datasetName());
            this.taskLoading = true;
            this.taskManagementService.getTasksByDatasetName(datasetName).pipe(
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
          }else {
            this.dataCarouselService.datasetName.set('');
          }
        });
        this.stuckStaging$.pipe(filter => filter).subscribe(dataset => {
          this.dataCarouselService.stagingDataset.set(dataset);
        });
    }

    formDashboardURL(filename: string): string{
      return  `https://monit-grafana.cern.ch/d/FtSFfwdmk/ddm-transfers?from=now-30d&orgId=17&to=now&var-activity=All&var-binning=$__auto_interval_binning&var-columns=src_cloud&var-dst_cloud=All&var-dst_endpoint=All&var-enr_filters=data.name%7C%3D%7C${filename}&var-groupby=dst_cloud&var-measurement=ddm_transfer&var-retention_policy=raw&var-src_cloud=All&var-src_country=All&var-src_endpoint=All&var-src_site=All&var-activity_disabled=Analysis%20Input&var-activity_disabled=Data%20Consolidation&var-activity_disabled=Deletion&var-activity_disabled=Functional%20Test&var-activity_disabled=Production%20Input&var-activity_disabled=Production%20Output&var-activity_disabled=Staging&var-activity_disabled=User%20Subscriptions&var-protocol=All&var-src_tier=All&var-src_token=All&var-dst_tier=All&var-dst_country=All&var-dst_site=All&var-dst_token=All&var-remote_access=All&var-include=&var-exclude=none&var-exclude_es=All&var-include_es_dst=All&var-include_es_src=All&var-rows=dst_cloud&viewPanel=129`
    }

}
