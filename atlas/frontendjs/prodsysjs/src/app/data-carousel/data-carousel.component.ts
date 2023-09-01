import {Component, OnInit, ViewChild} from '@angular/core';

import {RequestPerDay} from './request-per-day';

import {DataCarouselService} from './data-carousel.service';
import {MatLegacyPaginator as MatPaginator} from '@angular/material/legacy-paginator';
import {MatSort} from '@angular/material/sort';
import {MatLegacyTableDataSource as MatTableDataSource} from '@angular/material/legacy-table';


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
