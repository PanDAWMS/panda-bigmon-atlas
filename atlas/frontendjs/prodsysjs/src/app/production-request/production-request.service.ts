import { Injectable, signal, inject } from '@angular/core';
import {HttpClient, httpResource} from "@angular/common/http";
import {Observable, of, Subject} from "rxjs";
import {
  ProductionRequestBase,
  ProductionRequests,
  ProductionRequestsJiraInfo,
  RequestTransitions,
  Slice,
  SliceBase,
  Step
} from "./production-request-models";
import {catchError, map, tap} from "rxjs/operators";
import {ExtensionRequest} from "../derivation-exclusion/gp-deletion-container";


export interface AsyncProdTaskSplitProgress {
  processed: number;
  total?: number;
  progress?: number;
  name: string;
  currentResults: string[];
}

export interface  RulesResultInfo {
  dataset: string;
  return_code: string;
  return_info: string;
}
export interface AsyncProdTaskSplitStatus {
  status: string;
  progress: undefined| AsyncProdTaskSplitProgress;
  result: undefined | number[]| string[]|RulesResultInfo[]| string;
}

export interface StepPosition {
  request_id: number;
  slice_number: number;
  step_id: number;
}
export interface RequestCheckResult{
  type: "REQUEST" | "SLICE";
  step_position: StepPosition[];
  check_name: string;
  status: "ERROR" | "WARNING" | "PASSED";
  message?: string;
  details?: {[key: string]: any};
}


interface  ProductionRequestStats{
  [requestID: number]: {
    slices: number;
    input_events: number;
    input_datasets: number;
  };
}



export interface PMGRequestCheckResult {
  production_requests: ProductionRequestBase[];
  checks: RequestCheckResult[];
  approval_required: boolean;
}

export interface PMGApproveResult {
  requestIDs: number[];
  status: string;
}

export interface MCCampaignParam {
  campaign: string;
  subcampaign: string;
  step: string;
  project_base: string;
  energies: string[];
  parent_steps: [string, string, string][];
}

export interface MCCampaignSubmissionStructure {
  campaignProvenance: Record<string, Record<string, string>>;
  campaignParams: Record<string, MCCampaignParam>;
}

export interface MCCampaignSubmissionFirstStepFormValue {
  descriptionCtrl: string;
  refLinkCtrl: string;
  physGroupCtrl: string;
  longDescriptionCtrl: string;
  ccCtrl: string;
}

export interface MCCampaignSubmissionSpreadsheetConfigFormValue {
  campaignCtrl: string;
  subCampaignCtrl: string[];
  subCampaignEventRatiosCtrl: number[];
  startingStepCtrl: string;
  lastStepCtrl: string;
  energyCtrl: string;
  simulationTypeCtrl: string;
  spreadsheetCtrl: string;
  spreadsheetFileCtrl: File | null;
  useFirstSpreadsheetCtrl: boolean;
  ratioCtrl: number | null;
}

export interface MCCampaignSubmissionSpreadsheetConfig {
  campaign: string;
  sub_campaigns: string[];
  sub_campaign_event_ratios: number[];
  starting_step: string;
  last_step: string;
  energy: string;
  simulation_type: string;
  spreadsheet_url: string;
  spreadsheet_file_name: string;
  use_first_spreadsheet: boolean;
  ratio: number | null;
}

export interface MCCampaignsSubmissionRequest {
  description: string;
  ref_link: string;
  phys_group: string;
  long_description: string;
  cc: string;
  spreadsheet_configs: MCCampaignSubmissionSpreadsheetConfig[];
}

interface MCCampaignSubmissionStructureResponse {
  campaign_provenance: Record<string, Record<string, string>>;
  campaign_params: Record<string, MCCampaignParam>;
}

@Injectable({
  providedIn: 'root'
})
export class ProductionRequestService {
  private http = inject(HttpClient);


  private prSliceUrl = '/production_request/prepare_slice';
  private prStepsUrl = '/production_request/steps_for_requests';
  private prSaveSliceUrl = '/production_request/save_slice/';
  private prStepsJiraUrl = '/production_request/collect_steps_by_jira/';
  private prInfoJiraUrl = '/production_request/info_by_jira/';
  private prGetSplitRequestUrl = '/production_request/prepare_horizontal_transition/';
  private prDoHorizontalSplitUrl = '/production_request/submit_horizontal_transition/';
  private prAsyncTaskStatus = '/prodtask/celery_task_status_full/';
  private prPMGCheckUpUrl = '/production_request/pmg_request_verification';
  private prPMGApproveUrl = '/production_request/pmg_approve/';
  private mcCampaignSubmissionStructureUrl = '/production_request/mc_campaign_submission_structure/';
  private mcCampaignSubmissionProcessingUrl = '/production_request/mc_campaigns_submission_processing/';



  private sliceModificationSource = new Subject<Slice>();
  private sliceSavedSource = new Subject<Slice>();
  jiraForPMGCheckUp = signal('');
  sliceChanged$ = this.sliceModificationSource.asObservable();
  sliceSaved$ = this.sliceSavedSource.asObservable();

  getPMGCheckUPResource = httpResource<PMGRequestCheckResult>(() =>
    this.jiraForPMGCheckUp() ? `${this.prPMGCheckUpUrl}?jira=${this.jiraForPMGCheckUp()}` : '');

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
  setNewPMGStatus(jira: string, action: string): Observable<PMGApproveResult> {
    return this.http.post<PMGApproveResult>(this.prPMGApproveUrl, {jira, action});
  }

  getMCCampaignSubmissionStructure(): Observable<MCCampaignSubmissionStructure> {
    return this.http.get<MCCampaignSubmissionStructureResponse>(this.mcCampaignSubmissionStructureUrl).pipe(
      map((response) => ({
        campaignProvenance: response.campaign_provenance ?? {},
        campaignParams: response.campaign_params ?? {},
      }))
    );
  }

  submitMCCampaignsSubmission(
    firstStepFormGroupData: MCCampaignSubmissionFirstStepFormValue,
    spreadsheetConfigsData: MCCampaignSubmissionSpreadsheetConfigFormValue[]
  ): Observable<MCCampaignsSubmissionRequest> {
    const requestPayload: MCCampaignsSubmissionRequest = {
      description: String(firstStepFormGroupData.descriptionCtrl ?? ''),
      ref_link: String(firstStepFormGroupData.refLinkCtrl ?? ''),
      phys_group: String(firstStepFormGroupData.physGroupCtrl ?? ''),
      long_description: String(firstStepFormGroupData.longDescriptionCtrl ?? ''),
      cc: String(firstStepFormGroupData.ccCtrl ?? ''),
      spreadsheet_configs: spreadsheetConfigsData.map((config) => ({
        campaign: String(config.campaignCtrl ?? ''),
        sub_campaigns: Array.isArray(config.subCampaignCtrl) ? config.subCampaignCtrl.map((value) => String(value)) : [],
        sub_campaign_event_ratios: Array.isArray(config.subCampaignEventRatiosCtrl)
          ? config.subCampaignEventRatiosCtrl.map((value) => Number(value))
          : [],
        starting_step: String(config.startingStepCtrl ?? ''),
        last_step: String(config.lastStepCtrl ?? ''),
        energy: String(config.energyCtrl ?? ''),
        simulation_type: String(config.simulationTypeCtrl ?? ''),
        spreadsheet_url: String(config.spreadsheetCtrl ?? ''),
        spreadsheet_file_name: config.spreadsheetFileCtrl?.name ?? '',
        use_first_spreadsheet: Boolean(config.useFirstSpreadsheetCtrl),
        ratio: config.ratioCtrl == null ? null : Number(config.ratioCtrl),
      })),
    };

    const formData = new FormData();
    formData.append('request', JSON.stringify(requestPayload));
    spreadsheetConfigsData.forEach((config, index) => {
      if (config.spreadsheetFileCtrl) {
        formData.append(`spreadsheet_file_${index}`, config.spreadsheetFileCtrl, config.spreadsheetFileCtrl.name);
      }
    });

    return this.http.post<MCCampaignsSubmissionRequest>(this.mcCampaignSubmissionProcessingUrl, formData);
  }

  getAsyncTaskStatus(asyncTaskID: string): Observable<AsyncProdTaskSplitStatus> {
    return this.http.get<AsyncProdTaskSplitStatus>(this.prAsyncTaskStatus + asyncTaskID);
  }

  splitRequestHorizontaly(requestID: string, approve: boolean, patterns: {[key: string]: number }): Observable<string> {
    return this.http.post<string>(this.prDoHorizontalSplitUrl, {requestID, approve, patterns});
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
