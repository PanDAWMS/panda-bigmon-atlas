import {Component, OnInit} from '@angular/core';
import {DataCarouselService} from "../data-carousel.service";




@Component({
  selector: 'app-data-carousel-config',
  templateUrl: './data-carousel-config.component.html',
  styleUrls: ['./data-carousel-config.component.css']
})
export class DataCarouselConfigComponent implements OnInit {
  public dataCarouselConfig$ = this.dataCarouselService.getDataCarouselConfig();
  constructor(private  dataCarouselService: DataCarouselService) { }

  ngOnInit(): void {
  }
}
