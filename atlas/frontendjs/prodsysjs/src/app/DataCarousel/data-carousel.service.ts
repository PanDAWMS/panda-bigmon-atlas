import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";

export interface CarouselTapeConfig {
  tapeName: string;
  baseRule: string;
  active: boolean;
  min_bulksize: number;
  max_bulksize: number;
  batchdelay: number;
}

export interface CarouselConfig {
  carouselTapes: CarouselTapeConfig[];
  excludeSites: string[];
}

@Injectable({
  providedIn: 'root'
})
export class DataCarouselService {

  constructor(private http: HttpClient) { }
  private prDataCarouselConfigUrl = '/api/data_carousel_config/';

  getDataCarouselConfig() {
    return this.http.get<CarouselConfig>(this.prDataCarouselConfigUrl);
  }

}
