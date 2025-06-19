import {Component, computed, effect, inject, input, OnInit, Signal} from '@angular/core';
import {DataCarouselService, DatasetExistsResponse} from "../../DataCarousel/data-carousel.service";
import {DatePipe, JsonPipe} from "@angular/common";
import {MatProgressSpinner} from "@angular/material/progress-spinner";
import {HttpErrorResponse} from "@angular/common/http";
import {setErrorMessage} from "../../dsid-info/dsid-info.service";
import {RucioURLPipe} from "../../derivation-exclusion/rucio-url.pipe";
import {AppModule} from "../../app.module";
import {DatasetSizePipe} from "../../derivation-exclusion/dataset-size.pipe";
import {RouterLink} from "@angular/router";
import {RucioReplica, RucioRule} from "../production-request-models";
import {
  MatCell, MatCellDef,
  MatColumnDef,
  MatHeaderCell,
  MatHeaderCellDef,
  MatHeaderRow, MatHeaderRowDef,
  MatRow, MatRowDef,
  MatTable, MatTableDataSource
} from "@angular/material/table";
import {MatPaginator} from "@angular/material/paginator";
import {MatSort} from "@angular/material/sort";
import {StagingManagementComponent} from "../../DataCarousel/staging-management/staging-management.component";


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
    JsonPipe,
    MatTable,
    MatHeaderCell,
    MatCell,
    MatHeaderRow,
    MatRow,
    MatPaginator,
    MatSort,
    MatColumnDef,
    MatHeaderCellDef,
    MatCellDef,
    MatHeaderRowDef,
    MatRowDef,
    StagingManagementComponent
  ],
  templateUrl: './rucio-did.component.html',
  styleUrl: './rucio-did.component.css'
})
export class RucioDIDComponent implements OnInit {
    private dataCarouselService = inject(DataCarouselService);

    datasetName = input<string>();
    datasetExists = computed(() => this.dataCarouselService.datasetInfoResource.value()?.dataset_exists );
    datasetInfo: Signal<DatasetExistsResponse|undefined> = computed(() => {
      if (this.datasetExists()){
        return this.dataCarouselService.datasetInfoResource.value().dataset_knowledge as DatasetExistsResponse;
      }else {
        return undefined;
      }
    });
    displayedColumns: string[] = [
    'ruleRSEExpression',
    'ruleAccount',
    'replicaRSE',
    'replicaAvailableLength',
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
    constructor() {
      effect(() => {
      this.dataSource.data = this.rulesWithReplicas();

    });
    }

  ngOnInit(): void {
        this.dataCarouselService.datasetName.set(this.datasetName());
    }

}
