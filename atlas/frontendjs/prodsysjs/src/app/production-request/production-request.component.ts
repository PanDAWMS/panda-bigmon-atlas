import {Component, OnInit, AfterViewInit, ChangeDetectionStrategy, ChangeDetectorRef} from '@angular/core';
import {ProductionRequestService} from './production-request.service';
import {
  ProductionRequestBase,
  ProductionRequests,
  ProductionRequestsJiraInfo,
  ProductionTask,
  Slice, Step
} from './production-request-models';
import {FormControl} from "@angular/forms";
import {SelectionModel} from "@angular/cdk/collections";
import {MAT_CHECKBOX_DEFAULT_OPTIONS, MatCheckboxDefaultOptions} from "@angular/material/checkbox";
import {ActivatedRoute, ParamMap, Router} from "@angular/router";
import {Observable} from "rxjs";
import {debounceTime, distinctUntilChanged} from "rxjs/operators";





@Component({
  selector: 'app-production-request',
  templateUrl: './production-request.component.html',
  styleUrls: ['./production-request.component.css'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  providers: [
    {provide: MAT_CHECKBOX_DEFAULT_OPTIONS, useValue: { clickAction: 'noop' } as MatCheckboxDefaultOptions}
  ]
})
export class ProductionRequestComponent implements OnInit, AfterViewInit {

  constructor(private route: ActivatedRoute, private router: Router, private productionRequestService: ProductionRequestService, private cdr: ChangeDetectorRef) { }
  slices: Slice[];
  filteredSlices: Slice[] = undefined;
  requestByCampaign: Map<string, number[]> = new Map<string, number[]>();
  campaigns: string[] = [];
  chipsCampaignsControl = new FormControl([]);
  projects: string[] = [];
  chipsProjectsControl = new FormControl([]);
  taskStatusControl = new FormControl([]);
  mainFilter = new FormControl('');
  requestByProject: Map<string, number[]> = new Map<string, number[]>();
  lastChecked?: Slice = undefined;
  campaignColors = new Map<string, any>();
  productionRequests: Map<number, ProductionRequestBase> = new Map<number, ProductionRequestBase>();
  stepsOrder: string[] = undefined;
  allSteps: ProductionRequests;
  selectedSlices: SelectionModel<Slice>;
  totalFilteredTasks: {[status: string]: number} = {};
  filteredSelectedSlices: Slice[] = [];
  totalLen: number;
  sourceSteps: Observable<ProductionRequests>;
  colorSchema: {[index: number]: any} = {};
  stepFilterFormControl = new FormControl([]);
  originalTaskStatus: {[status: string]: number} = {};
  jira: string;
  selectedTasksStep: Step;
  selectedTasksSlice: Slice;
  modifiedSlices: Partial<Slice>[] = [];
  pageInfo: ProductionRequestsJiraInfo;
  ngOnInit(): void {
    const stepPosition: {[index: string]: number} = {};
    this.route.paramMap.subscribe(params => {
      this.jira = params.get('jira');
      if (params.get('jira')){
        this.sourceSteps = this.productionRequestService.getStepsJira(params.get('jira'));
        this.productionRequestService.getInfoJira(params.get('jira')).subscribe(info => this.pageInfo = info);
      }
      if (params.get('reqIDs')) {
        this.sourceSteps = this.productionRequestService.getSteps(params.get('reqIDs'));
      }
      this.sourceSteps.subscribe(allSteps => {
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
          this.productionRequests.set(allSteps.production_requests[i].reqid, allSteps.production_requests[i]);
          const campaign = allSteps.production_requests[i].campaign + ':' + allSteps.production_requests[i].subcampaign;
          if (this.requestByCampaign.has(campaign)){
            const currentCampaigns = this.requestByCampaign.get(campaign);
            currentCampaigns.push(allSteps.production_requests[i].reqid);
            this.requestByCampaign.set(campaign, currentCampaigns);
          }
          else {
            this.requestByCampaign.set(campaign, [allSteps.production_requests[i].reqid]);
            this.campaigns.push(campaign);
          }
          if (this.requestByProject.has(allSteps.production_requests[i].project_id)){
            const currentProjects = this.requestByProject.get(allSteps.production_requests[i].project_id);
            currentProjects.push(allSteps.production_requests[i].reqid);
            this.requestByProject.set(allSteps.production_requests[i].project_id, currentProjects);
          }
          else {
            this.requestByProject.set(allSteps.production_requests[i].project_id, [allSteps.production_requests[i].reqid]);
            this.projects.push(allSteps.production_requests[i].project_id);
          }

          this.colorSchema[allSteps.production_requests[i].reqid] = {campaign : campaign.replace(':', '_'),
            project: allSteps.production_requests[i].project_id };
        }
        this.chipsCampaignsControl = new FormControl(this.campaigns);
        this.chipsProjectsControl = new FormControl(this.projects);

        this.stepsOrder = [];
        this.stepsOrder = Object.entries(stepPosition).sort(([, a], [, b]) => a - b)
          .reduce( (b, c) => b.concat(c[0]), []);
        this.slices = allSteps.slices;
        this.selectedTasksSlice = this.slices[0];
        this.selectedTasksStep = this.selectedTasksSlice.steps[0];
        this.totalLen = this.slices.length;
        this.filteredSlices = allSteps.slices;
        this.selectedSlices = new SelectionModel<Slice>(true, []);
        this.selectedSlices.select(...this.filteredSlices);
        this.recountFiltered();
        this.originalTaskStatus = this.totalFilteredTasks;
        this.taskStatusControl = new FormControl(Object.keys(this.totalFilteredTasks));
        this.chipsCampaignsControl.valueChanges.subscribe( newValues => {
          this.setFilterSortFragment('campaigns', newValues.toString());
          this.filterSlices();
        });
        this.chipsProjectsControl.valueChanges.subscribe( newValues => {
          this.setFilterSortFragment('projects', newValues.toString());
          this.filterSlices();
        });
        this.taskStatusControl.valueChanges.subscribe( newValues => {
          // this.setFilterSortFragment('tasks', newValues.toString());
          //console.log(newValues);
          this.filterSlices();
        });
        this.stepFilterFormControl.valueChanges.subscribe( newValues => {
          this.setFilterSortFragment('steps', newValues.toString());
          this.filterSlices();
        });
        this.mainFilter.valueChanges.pipe( debounceTime(300), distinctUntilChanged())
          .subscribe(filterValue => {
            this.setFilterSortFragment('filter', filterValue);
            this.filterSlices();
          });
        this.productionRequestService.sliceChanged$.subscribe( changeSlice => {
          const existedIndex = this.slices.findIndex( slice => slice.id === changeSlice.id);
          if (existedIndex > -1 ){
            for (const [key, value] of Object.entries(changeSlice)) {
              if ((typeof this.slices[existedIndex][key] in ['string', 'number', 'boolean']) &&
                (this.slices[existedIndex][key] === undefined) || (this.slices[existedIndex][key] !== value)) {
                if (this.slices[existedIndex].modifiedFields === undefined){
                  this.slices[existedIndex].modifiedFields = {};
                }
                this.slices[existedIndex].modifiedFields[key] = value;
              }
            }
          } else {
            this.slices.push(changeSlice);
          }
        });
        this.productionRequestService.sliceSaved$.subscribe( savedSlice => {
          console.log(savedSlice);
          const existedIndex = this.slices.findIndex( slice => slice.id === savedSlice.id);
          if (existedIndex > -1 ){
            console.log(existedIndex);
            const steps = this.slices[existedIndex].steps;
            this.slices[existedIndex] = {...savedSlice};
            this.slices[existedIndex].steps = steps;
            this.slices[existedIndex].modifiedFields = {};
          } else {
            this.slices.push(savedSlice);
          }
          this.filterSlices();
        });
        this.cdr.detectChanges();

      });
    });


  }
  setFilterSortFragment(fragment: string, value: string): void{
    if (value !== ''){
      this.router.navigate(['.'],
        { queryParams: {[fragment]: value}, queryParamsHandling: 'merge' , relativeTo: this.route });
    } else {
      this.router.navigate(['.'],
        { queryParams: {[fragment]: null}, queryParamsHandling: 'merge' , relativeTo: this.route });
    }
  }
  ngAfterViewInit(): void{
      this.route.queryParamMap.subscribe((paramMap: ParamMap) => {
        if (paramMap.get('campaigns')){
          this.chipsCampaignsControl.setValue(paramMap.get('campaigns'));
        }

        if (paramMap.get('projects')){
          this.chipsProjectsControl.setValue(paramMap.get('projects'));
        }
        if (paramMap.get('filter')){
          this.mainFilter.setValue(paramMap.get('filter'));
        }
      });

  }

  recountFiltered(): void{
    this.filteredSelectedSlices = this.filteredSlices.filter(slice => this.selectedSlices.isSelected(slice));
    this.totalFilteredTasks = {};
    const taskCounter = (accum: {}, currentSlice: Slice) => {
      for (const [status, value] of Object.entries(currentSlice.tasksByStatus)){
        if (status in accum){
          accum[status] += value;
        } else {
          accum[status] = value;
        }
      }
      return accum;
    };
    this.totalFilteredTasks = this.filteredSlices.reduce(taskCounter, {} );
  }
  masterCheckboxToggle(): void{
    if (this.filteredSelectedSlices.length === 0){
      this.selectedSlices.select(...this.filteredSlices);
    } else {
      this.selectedSlices.deselect(...this.filteredSlices);
    }
    this.recountFiltered();
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
    this.filteredSlices = this.slices.filter((slice) => this.sliceHasCampaign(slice));
    this.lastChecked = undefined;
    this.recountFiltered();
    this.cdr.detectChanges();

  }
  unFilterSlice(): void{
    this.filteredSlices = this.slices;
    this.cdr.detectChanges();

  }

  private sliceHasCampaign(slice: Slice): boolean {
    if (this.mainFilter.value.length > 0){
      const filterString = slice.input_data + slice.comment + slice.dataset;
      if (filterString.indexOf(this.mainFilter.value) === -1){
        return false;
      }
    }
    if (this.taskStatusControl.value.length > 0){
      const tasksSlices = Object.keys(slice.tasksByStatus);
      const intersection = tasksSlices.filter(value => this.taskStatusControl.value.includes(value));
      if ( intersection.length === 0){
        return false;
      }
    }


    for (const step of slice.steps){
      const productionRequest = this.productionRequests.get(step.request_id);
      if ((this.chipsCampaignsControl.value.indexOf(productionRequest.campaign + ':' + productionRequest.subcampaign) > -1) &&
        (this.chipsProjectsControl.value.indexOf(productionRequest.project_id) > -1)){
        return true;
      }
    }
    return false;
  }

  toggleSliceChange($event: Event, slice: Slice): void {
    let isShift = false;
    if (  ($event as MouseEvent).shiftKey ){
      isShift = ($event as MouseEvent).shiftKey;
    }
    if (isShift && (this.lastChecked !== undefined) && (this.filteredSlices.indexOf(this.lastChecked) > -1)){
      let [startIndex, stopIndex] = [this.filteredSlices.indexOf(this.lastChecked), this.filteredSlices.indexOf(slice) + 1];
      if (startIndex >= stopIndex){
        [startIndex, stopIndex] = [stopIndex - 1, startIndex + 1 ];
      }
      if (this.selectedSlices.isSelected(slice)){
        this.selectedSlices.deselect(...this.filteredSlices.slice(startIndex, stopIndex));
      } else {
        this.selectedSlices.select(...this.filteredSlices.slice(startIndex, stopIndex));
      }

    } else {
      this.selectedSlices.toggle(slice);
    }
    this.lastChecked = slice;
    this.recountFiltered();
    return null;
  }


  onTaskSelected(step: Step, slice: Slice): void {
    this.selectedTasksSlice = slice;
    this.selectedTasksStep = step;
  }
}
