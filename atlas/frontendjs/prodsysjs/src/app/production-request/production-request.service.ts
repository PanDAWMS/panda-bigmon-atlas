import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable, of} from "rxjs";
import {ProductionRequestSliceSteps, Slice, Step} from "./production-request-models";
import {map} from "rxjs/operators";

@Injectable({
  providedIn: 'root'
})
export class ProductionRequestService {

  private prSliceUrl = '/production_request/prepare_slice';
  private prStepsUrl = '/production_request/steps_for_requests';

  constructor(
    private http: HttpClient){}

  getSlice(id: string): Observable<Slice> {
    return this.http.get<Slice>(this.prSliceUrl, {params: {slice_id: id }});
  }
  getSteps(productionRequestsIDs: string): Observable<ProductionRequestSliceSteps> {
    return this.http.get<ProductionRequestSliceSteps>(this.prStepsUrl, {params: {requests_list: productionRequestsIDs }})
      .pipe(map( allSteps => this.assembleSlices(allSteps)));
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

  private assembleSlices(allSteps: ProductionRequestSliceSteps): ProductionRequestSliceSteps {
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
    allSteps.steps = [];
    return allSteps;
  }
}
