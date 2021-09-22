import {Component, OnInit, AfterViewInit, ChangeDetectionStrategy, ChangeDetectorRef} from '@angular/core';
import {ProductionRequestService} from './production-request.service';
import {ProductionRequestSliceSteps, Slice} from './production-request-models';

@Component({
  selector: 'app-production-request',
  templateUrl: './production-request.component.html',
  styleUrls: ['./production-request.component.css'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ProductionRequestComponent implements OnInit, AfterViewInit {

  constructor(private productionRequestService: ProductionRequestService, private cdr: ChangeDetectorRef) { }
  slices: Slice[];
  filteredSlices: Slice[] = undefined;
  stepsOrder: string[] = undefined;
  allSteps: ProductionRequestSliceSteps;
  requestColors = [{background: 'red'}, {background: 'green'}, {background: 'yellow'}, {background: 'orange'}, {background: 'blue'}
    , {background: 'grey'}];
  totalLen: number;
  colorSchema: {[index: number]: any} = {};
  ngOnInit(): void {
    // this.productionRequestService.getSlice('1').subscribe(slice => {this.slice = slice; });
    const stepPosition: {[index: string]: number} = {};
    this.productionRequestService.getSteps('37996,38035,37997,38037,38039,37998,38040').subscribe(allSteps => {
    // this.productionRequestService.getSteps('11881').subscribe(allSteps => {
      for (const slice of  allSteps.slices){
        for (let i = 0; i < slice.steps.length; i++){
          if (slice.steps[i].step_name in stepPosition){
            if (i > stepPosition[slice.steps[i].step_name]){
              stepPosition[slice.steps[i].step_name] = i;
            }
          } else {
            stepPosition[slice.steps[i].step_name] = i;
          }
        }
      }
      for (let i = 0; i < allSteps.production_requests.length; i++){
        this.colorSchema[allSteps.production_requests[i].reqid] = this.requestColors[i % this.requestColors.length];
      }
      this.stepsOrder = [];
      this.stepsOrder = Object.entries(stepPosition).sort(([, a], [, b]) => a - b)
        .reduce( (b, c) => b.concat(c[0]), []);
      // this.stepsOrder = undefined;
      this.slices = allSteps.slices;
      this.totalLen = this.slices.length;
      this.filteredSlices = allSteps.slices;
      this.cdr.detectChanges();

    });
  }

  ngAfterViewInit(): void{
  }
  sortSlicesByDataset(): void {
    this.filteredSlices.sort( (a, b) =>  a.input_data.localeCompare(b.input_data));
    this.filteredSlices = [...this.filteredSlices];
    this.cdr.detectChanges();

  }
  sortSlicesByID(): void {
    this.filteredSlices.sort( (a, b) =>  a.id - b.id);
    this.filteredSlices = [...this.filteredSlices];
    this.cdr.detectChanges();

  }
  sliceTrackBy(index, item): number{
    return item.id;
  }
  sliceHasRequest(slice: Slice, requestID: number): boolean{
    for (const step of slice.steps){
      if (step.request_id === requestID){
        return true;
      }
    }
    return false;
  }

  filterSlices(): void{
    this.filteredSlices = this.slices.filter((slice) => this.sliceHasRequest(slice, 38039));
    this.cdr.detectChanges();

  }
  unFilterSlice(): void{
    this.filteredSlices = this.slices;
    this.cdr.detectChanges();

  }

}
