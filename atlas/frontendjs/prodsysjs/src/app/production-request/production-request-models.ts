export interface SliceBase {
  id: number;
  dataset: string;
  slice: number;
  brief: string;
  phys_comment: string;
  comment: string;
  input_data: string;
  project_mode: string;
  priority: number;
  input_events: number;
  is_hide: boolean;
  request: any;
  cloned_from: any;
}

export interface Slice extends SliceBase{
  steps: Step[];
  tasksByStatus?: {[status: string]: number};
  modifiedFields?: Partial<SliceBase>;
}
export interface ProductionRequestsJiraInfo {
  reqIDs: number[];
  requests_number: number;
  description: string;
  jira_reference: string;
  manager: string;
  phys_group: string;
}

export interface ProductionRequestBase {
  reqid: number;
  manager: string;
  description: string;
  ref_link: string;
  cstatus: string;
  provenance: any;
  request_type: any;
  campaign: string;
  subcampaign: string;
  phys_group: any;
  energy_gev: number;
  is_error: boolean;
  jira_reference: string;
  info_fields: string;
  is_fast: boolean;
  project_id: string;

  project?: string;

}

export interface ProductionRequests {
  production_requests: ProductionRequestBase[];
  steps: Step[];
  slices: Slice[];
}




export interface StepTaskConfig{
  nEventsPerJob?: number;
  nEventsPerMergeJob?: number;
  nFilesPerMergeJob?: number;
  nGBPerMergeJob?: number;
  nMaxFilesPerMergeJob?: number;
  nFilesPerJob?: number;
  nGBPerJob?: number;
  maxAttempt?: number;
  nEventsPerInputFile?: number;
  maxFailure?: number;
  split_slice?: number;
  input_format?: string;
  token?: string;
  merging_tag?: string;
  project_mode?: string;
  evntFilterEff?: string;
  PDA?: string;
  PDAParams?: string;
  container_name?: string;
  onlyTagsForFC?: string;
  previous_task_list?: number[];
}

export interface StepBase{
  id: number;
  status: string;
  priority: number;
  input_events: number;
  task_config: StepTaskConfig;
  // project_mode: string;
  request_id: number;
  // step_template: any;
  slice_id: number;
  step_parent_id: any;
  ami_tag: string;
  output_formats: string;
  step_name: string;
  production_step_parent_id?: number;
  production_step_parent_request_id?: number;
  production_step_parent_slice?: number;
}

export interface Step extends StepBase{

  // step_actions: StepAction[]|null;
  tasks?: ProductionTask[];
  tasksByStatus?: {[status: string]: number};
  campaign?: string;
  subcampaign?: string;
  project_id?: string;
  modifiedFields?: Partial<SliceBase>;

}


export interface StepAction{
  id: number;
  step: number;
  action: number;
  create_time: string;
  execution_time: string;
  done_time: string;
  message: string;
  attempt: number;
  status: string;
  config: string;
  request: any;
}

export interface StagingProgress{
  dataset: string;
  rule: string;
  source: string;
  staged_files: number;
  total_files: number;
  status: string;

}

export interface ProductionTask{
  id: number;
  parent_id: number;
  chain_tid: number;
  name: string;
  project: string;
  username: string;
  dsn: string;
  phys_short: string;
  simulation_type: string;
  phys_group: string;
  provenance: string;
  status: string;
  total_events: number;
  total_req_events: number;
  total_req_jobs: number;
  total_done_jobs: number;
  submit_time: string;
  start_time: string;
  timestamp: string;
  pptimestamp: string;
  postproduction: string;
  priority: number;
  current_priority: number;
  update_time: string;
  update_owner: string;
  comments: string;
  inputdataset: string;
  physics_tag: string;
  reference: string;
  campaign: string;
  jedi_info: string;
  total_files_failed: number;
  total_files_tobeused: number;
  total_files_used: number;
  total_files_onhold: number;
  is_extension: boolean;
  total_files_finished: number;
  ttcr_timestamp: string;
  ttcj_timestamp: string;
  ttcj_update_time: string;
  primary_input: string;
  ami_tag: string;
  output_formats: string;
  request_id: number;
  coreCount?: number;
  subcampaign?: string;
  projectMode?: string;
  failureRate?: number;
  hashtags?: string[];
  staging?: StagingProgress;
  inputEvents?: number;
  step_name?: string;
  // step: any;
  // request: any;
}

export interface RequestTransitions{
  request: ProductionRequestBase;
  print_results: {name: string, transitions: string[]}[];
  patterns: { [key: string]: number };
  long_description: string;
  number_of_slices: number;
  all_patterns: {id: number, pattern: string}[];
  async_task_id: string;
}
export interface JEDITask{
  id: number;
  taskname: string;
  gshare: string;
  splitrule: string;
  cputime: number;
  cputimeunit: string;
  cpuefficiency: number;
  walltime: number;
  walltimeunit: string;
  outdiskcount: number;
  outdiskunit: string;
  workdiskcount: number;
  workdiskunit: string;
  ramcount: number;
  ramunit: string;
  iointensity: number;
  iointensityunit: string;
  diskio: number;
  diskiounit: string;
  basewalltime: number;
  errordialog: string;
}
