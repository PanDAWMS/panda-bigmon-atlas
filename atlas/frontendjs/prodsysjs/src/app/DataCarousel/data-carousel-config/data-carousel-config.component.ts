import { Component, OnInit, inject } from '@angular/core';
import {DataCarouselService} from "../data-carousel.service";




@Component({
    selector: 'app-data-carousel-config',
    templateUrl: './data-carousel-config.component.html',
    styleUrls: ['./data-carousel-config.component.css'],
    standalone: false
})
export class DataCarouselConfigComponent implements OnInit {
  private dataCarouselService = inject(DataCarouselService);

  public dataCarouselConfig$ = this.dataCarouselService.getDataCarouselConfig();

  ngOnInit(): void {
  }
}
