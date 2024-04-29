import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable, of, Subject} from "rxjs";
import {
  ProductionRequests,
  ProductionRequestsJiraInfo,
  RequestTransitions,
  Slice,
  SliceBase,
  Step
} from "./production-request-models";
import {catchError, map, tap} from "rxjs/operators";
import {ExtensionRequest} from "../derivation-exclusion/gp-deletion-container";

@Injectable({
  providedIn: 'root'
})
export class ProductionRequestService {

  constructor(
    private http: HttpClient){}

  private prSliceUrl = '/production_request/prepare_slice';
  private prStepsUrl = '/production_request/steps_for_requests';
  private prSaveSliceUrl = '/production_request/save_slice/';
  private prStepsJiraUrl = '/production_request/collect_steps_by_jira/';
  private prInfoJiraUrl = '/production_request/info_by_jira/';
  private prGetSplitRequestUrl = '/production_request/prepare_horizontal_transition/';
  private prDoHorizontalSplitUrl = '/production_request/submit_horizontal_transition/';

  private sliceModificationSource = new Subject<Slice>();
  private sliceSavedSource = new Subject<Slice>();
  sliceChanged$ = this.sliceModificationSource.asObservable();
  sliceSaved$ = this.sliceSavedSource.asObservable();

  private static countTasks(step: Step): {[status: string]: number} {
    const tasksByStatus = {};
    for (const task of step.tasks){
      if (task.status in tasksByStatus){
        tasksByStatus[task.status] += 1;
      } else {
        tasksByStatus[task.status] = 1;
      }
    }
    return tasksByStatus;
  }

  getSlice(id: string): Observable<Slice> {
    return this.http.get<Slice>(this.prSliceUrl, {params: {slice_id: id }});
  }
  getSteps(productionRequestsIDs: string): Observable<ProductionRequests> {
    return this.http.get<ProductionRequests>(this.prStepsUrl, {params: {requests_list: productionRequestsIDs }})
      .pipe(map( allSteps => this.assembleSlices(allSteps)));
  }
  getStepsJira(jiraID: string): Observable<ProductionRequests> {
    return this.http.get<ProductionRequests>(this.prStepsJiraUrl, {params: {jira: jiraID }})
      .pipe(map( allSteps => this.assembleSlices(allSteps)));
  }
  getInfoJira(jiraID: string): Observable<ProductionRequestsJiraInfo> {
    return this.http.get<ProductionRequestsJiraInfo>(this.prInfoJiraUrl, {params: {jira: jiraID }});
  }
  getSplitByCampaign(requestID: string): Observable<RequestTransitions> {
    return this.http.post<RequestTransitions>(this.prGetSplitRequestUrl, {requestID});
  }

  splitRequestHorizontaly(requestID: string, approve: boolean, patterns: {[key: string]: number }): Observable<number[]> {
    return this.http.post<number[]>(this.prDoHorizontalSplitUrl, {requestID, approve, patterns});
  }
  private handleError<T>(operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {

      // TODO: send the error to remote logging infrastructure
      console.error(error); // log to console instead

      // TODO: better job of transforming error for user consumption
      this.log(`${operation} failed: ${error.message}`);

      // Let the app keep running by returning an empty result.
      return of(result as T);
    };
  }

  private log(message: string): void {
    console.log(`ProductionRequestService: ${message}`);
  }

  private assembleSlices(allSteps: ProductionRequests): ProductionRequests {
    const sliceMap = new Map<number, Slice>(allSteps.slices.map(x => [x.id, x] as [number, Slice]));
    const firstSliceParentStep = new Map<number, number>();
    const lastSliceStep =  new Map<number, number>();
    const assembledSlices =  new Map<number, Slice>();
    const processedSteps = new Map<number, number>();
    let currentSliceID: number;
    let currentSlice: Slice;
    let taleSlice: Slice;
    for (const step of allSteps.steps){

      if ((step.step_parent_id !== undefined) && (step.step_parent_id !== step.id)){
        if (lastSliceStep.has(step.step_parent_id)){
          currentSliceID = lastSliceStep.get(step.step_parent_id);
          lastSliceStep.delete(step.step_parent_id);
        } else {
          currentSliceID = step.id;
        }
      } else {
        currentSliceID = step.id;
      }
      if (assembledSlices.has(currentSliceID)){
        currentSlice = assembledSlices.get(currentSliceID);
      } else {
        currentSlice = { ...sliceMap.get(step.slice_id)};
        currentSlice.steps = [];
        if ((step.step_parent_id !== undefined) && (step.step_parent_id !== step.id)){
          if (!firstSliceParentStep.has(step.step_parent_id)){
            firstSliceParentStep.set(step.step_parent_id, currentSliceID);
          }
        }
      }
      currentSlice.steps.push(step);
      if (firstSliceParentStep.has(step.id)){
        const taleSliceID = firstSliceParentStep.get(step.id);
        taleSlice =  assembledSlices.get(taleSliceID);
        currentSlice.steps = currentSlice.steps.concat(taleSlice.steps);
        lastSliceStep.set(currentSlice.steps[currentSlice.steps.length - 1].id, currentSliceID);
        if (lastSliceStep.has(step.id)){
          lastSliceStep.delete(step.id);
        }
        firstSliceParentStep.delete(step.id);
        assembledSlices.delete(taleSliceID);
      } else {
        lastSliceStep.set(step.id, currentSliceID);
      }
      assembledSlices.set(currentSliceID, { ...currentSlice});
      processedSteps.set(step.id, currentSliceID);
      step.tasksByStatus = ProductionRequestService.countTasks(step);
    }
    for (const [parentStep, sliceID] of firstSliceParentStep){
      if (processedSteps.has(parentStep)){
        const slice = assembledSlices.get(processedSteps.get(parentStep));
        const childSlice =  assembledSlices.get(processedSteps.get(sliceID));
        const assembledSteps = [] as Step[];
        let i = 1;
        for (; (i <=  slice.steps.length) && (slice.steps[i - 1].id !== parentStep); i++) {
          assembledSteps.push(slice.steps[i - 1]);
        }
        assembledSteps.push(slice.steps[i - 1]);
        childSlice.steps = assembledSteps.concat(childSlice.steps);
        assembledSlices.set(sliceID, { ...childSlice});
      }
    }
    allSteps.slices = Array.from(assembledSlices.values());
    for (const slice of allSteps.slices){
      const tasksByStatus = {};
      for (const step of slice.steps){
        if (step.tasksByStatus !== undefined){
          for ( const [status, value] of Object.entries(step.tasksByStatus)){
            if (status in tasksByStatus){
              tasksByStatus[status] += value;
            } else {
              tasksByStatus[status] = value;
            }
          }
        }
        slice.tasksByStatus = tasksByStatus;
      }
    }
    allSteps.steps = [];
    return allSteps;
  }

  modifySlice(slice: Slice): void{
    this.sliceModificationSource.next(slice);
  }

  saveSlice(slice: Slice): Observable<Slice> {
    return this.http.post<Slice>(this.prSaveSliceUrl, slice).pipe(
      tap(savedSlice => {
        this.log('Save slice');
        this.sliceSavedSource.next(savedSlice);
      }),
      catchError(this.handleError<Slice>('askExtension'))
    );
  }


}
