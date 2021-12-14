import { Component, OnInit } from '@angular/core';
import {UnmergedDatasetsCombined} from "./unmerge-cleaning.service";
import {ActivatedRoute} from "@angular/router";
import {UnmergeCleaningResolver} from "./unmerge-cleaning.resolver";



export interface DatasetToDelete {
  name: string;
  bytes: number;
  task_id: number;
  parentPer: number;
  daysLeft: number;
  parent_task_id: number;
}

@Component({
  selector: 'app-unmerge-cleaning',
  templateUrl: './unmerge-cleaning.component.html',
  styleUrls: ['./unmerge-cleaning.component.css']
})
export class UnmergeCleaningComponent implements OnInit {

  constructor(private route: ActivatedRoute) { }
  unmergedDatasets: UnmergedDatasetsCombined;
  unmergedDatasetsByFormat: {format: string, size: number, datasets: number}[] = [];
  prefix: string;

  ngOnInit(): void {
    this.prefix = this.route.snapshot.paramMap.get('prefix');
    this.unmergedDatasets =  this.route.snapshot.data.unmergedDatasets;
    for (const [key, value] of Object.entries(this.unmergedDatasets.outputs)){
      const totalSize = value.reduce((sum, current) => sum + current.bytes, 0);
      const totalDatasets = value.length;
      this.unmergedDatasetsByFormat.push({format: key, size: totalSize, datasets: totalDatasets});
    }

  }

}
