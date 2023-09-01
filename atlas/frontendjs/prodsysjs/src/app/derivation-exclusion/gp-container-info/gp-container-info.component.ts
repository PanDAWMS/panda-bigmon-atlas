import { Component, OnInit } from '@angular/core';
import {ActivatedRoute} from "@angular/router";
import {ContainerAllInfo, Dataset, Extension, GpContainerInfoService} from "./gp-container-info.service";
import {MatTableDataSource} from "@angular/material/table";

@Component({
  selector: 'app-gp-container-info',
  templateUrl: './gp-container-info.component.html',
  styleUrls: ['./gp-container-info.component.css']
})
export class GpContainerInfoComponent implements OnInit {
  container: string;
  containerFullInfo: ContainerAllInfo |undefined;
  mainContainerDetailsDataSource: MatTableDataSource<Dataset>;
  mainContainerExtensionsDataSource: MatTableDataSource<Extension>;
  dataType: string;
  outputFormat: string;
  amiTag: string;


  constructor(private route: ActivatedRoute, private gpContainerInfoService: GpContainerInfoService) { }

  ngOnInit(): void {
    this.mainContainerDetailsDataSource = new  MatTableDataSource<Dataset>();

    this.container = this.route.snapshot.paramMap.get('container');
    this.containerFullInfo = this.route.snapshot.data.gpContainerInfo;
    this.outputFormat = this.containerFullInfo.main_container.details.output_format;
    this.amiTag = this.containerFullInfo.main_container.details.ami_tag;
    if (this.container.startsWith('data')){
      this.dataType = 'data';
    } else {
      this.dataType = 'mc';
    }
  }
}
