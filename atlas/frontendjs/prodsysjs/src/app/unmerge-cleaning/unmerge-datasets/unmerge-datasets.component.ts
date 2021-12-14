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
  message: string;
  childTag: string;
  parentTag: string;
  unmergedDatasets: UnmergedDatasetsCombined;
  @ViewChild(MatPaginator) paginator: MatPaginator;
  @ViewChild(MatSort) sort: MatSort;

  constructor(private route: ActivatedRoute, ) { }
  ngOnInit(): void {

    this.prefix = this.route.snapshot.paramMap.get('prefix');
    if (this.prefix !== 'special'){
      this.unmergedDatasets =  this.route.snapshot.data.unmergedDatasets;
      this.outputFormat = this.route.snapshot.paramMap.get('output');
      this.datasetToDelete = new MatTableDataSource<DatasetToDelete>(this.unmergedDatasets.outputs[this.outputFormat]);

      this.message = `Unmerged not yet deleted ${this.prefix} ${this.outputFormat}`;
    }
    else {
      [this.parentTag, this.childTag] = [this.route.snapshot.paramMap.get('parentTag'), this.route.snapshot.paramMap.get('childTag')];
      this.unmergedDatasets =  this.route.snapshot.data.specialDatasets;
      this.datasetToDelete = new MatTableDataSource<DatasetToDelete>(this.unmergedDatasets.outputs["special"]);

      this.message = `Dataset not yet deleted for ${this.parentTag} - ${this.childTag}`;
    }
    this.numberOfDatasets = this.datasetToDelete.data.length;
    this.totalSize = this.datasetToDelete.data.reduce((sum, current) => sum + current.bytes, 0);
    this.collected = this.unmergedDatasets.timestamp;
  }
  ngAfterViewInit(): void {
    this.datasetToDelete.paginator = this.paginator;
    this.datasetToDelete.sort = this.sort;
  }

}
