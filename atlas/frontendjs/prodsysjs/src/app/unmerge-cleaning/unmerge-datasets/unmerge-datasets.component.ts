import {AfterViewInit, Component, OnInit, ViewChild} from '@angular/core';
import {MatTableDataSource} from "@angular/material/table";
import {DatasetToDelete} from "../unmerge-cleaning.component";
import {UnmergedDatasetsCombined} from "../unmerge-cleaning.service";
import {ActivatedRoute} from "@angular/router";
import {reduce} from "rxjs/operators";
import {MatPaginator} from "@angular/material/paginator";
import {MatSort} from "@angular/material/sort";

@Component({
  selector: 'app-unmerge-datasets',
  templateUrl: './unmerge-datasets.component.html',
  styleUrls: ['./unmerge-datasets.component.css']
})
export class UnmergeDatasetsComponent implements OnInit, AfterViewInit {

  datasetToDelete: MatTableDataSource<DatasetToDelete>;
  numberOfDatasets: number;
  totalSize: number;
  outputFormat: string;
  prefix: string;
  collected: string;
  unmergedDatasets: UnmergedDatasetsCombined;
  @ViewChild(MatPaginator) paginator: MatPaginator;
  @ViewChild(MatSort) sort: MatSort;

  constructor(private route: ActivatedRoute, ) { }
  ngOnInit(): void {
    this.outputFormat = this.route.snapshot.paramMap.get('output');
    this.prefix = this.route.snapshot.paramMap.get('prefix');
    this.unmergedDatasets =  this.route.snapshot.data.unmergedDatasets;
    this.datasetToDelete = new MatTableDataSource<DatasetToDelete>(this.unmergedDatasets.outputs[this.outputFormat]);
    this.numberOfDatasets = this.datasetToDelete.data.length;
    this.totalSize = this.datasetToDelete.data.reduce((sum, current) => sum + current.bytes, 0);
    this.collected = this.unmergedDatasets.timestamp;
  }
  ngAfterViewInit(): void {
    this.datasetToDelete.paginator = this.paginator;
    this.datasetToDelete.sort = this.sort;
  }

}
