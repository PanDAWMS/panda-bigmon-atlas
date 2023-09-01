import {Component, OnInit, ViewChild} from '@angular/core';

import {RequestPerDay} from './request-per-day';

import {DataCarouselService} from './data-carousel.service';
import {MatPaginator} from '@angular/material/paginator';
import {MatSort} from '@angular/material/sort';
import {MatTableDataSource} from '@angular/material/table';


@Component({
  selector: 'app-data-carousel',
  templateUrl: './data-carousel.component.html',
  styleUrls: ['./data-carousel.component.css']
})


export class DataCarouselComponent implements OnInit {
  stagingRequests: RequestPerDay[];
  columnsToDisplay = [ 'Files', 'Tape'];
  dataSource: MatTableDataSource<RequestPerDay>;

  @ViewChild(MatPaginator) paginator: MatPaginator;
  @ViewChild(MatSort) sort: MatSort;


  constructor(private dataCarouselService: DataCarouselService) { }

  ngOnInit(): void {
    this.dataSource = new MatTableDataSource();

    this.getStagingRequests();

    this.dataSource.sort = this.sort;

  }
  getStagingRequests(): void{
    this.dataCarouselService.getRequestsPerDay().subscribe(stagingRequests => {
          this.stagingRequests = stagingRequests;
          this.dataSource.data = stagingRequests;
          this.dataSource.sort = this.sort;
  });
  }

}
