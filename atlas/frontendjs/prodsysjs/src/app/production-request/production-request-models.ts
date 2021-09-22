export interface Slice{
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
  steps: Step[];
}

export interface ProductionRequestSliceSteps {
  production_requests: ProductionRequest[];
  steps: Step[];
  slices: Slice[];
}

export interface ProductionRequest{
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
  project: any;
}

export interface Step{
  id: number;
  status: string;
  priority: number;
  input_events: number;
  task_config: any;
  // project_mode: string;
  request_id: number;
  // step_template: any;
  slice_id: number;
  step_parent_id: any;
  ami_tag: string;
  output_formats: string;
  step_name: string;
  // step_actions: StepAction[]|null;
  tasks: ProductionTask[]|null;
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

export interface ProductionTask{
  id: number;
  // parent_id: number;
  // chain_tid: number;
  // name: string;
  // project: string;
  // username: string;
  // dsn: string;
  // phys_short: string;
  // simulation_type: string;
  // phys_group: string;
  // provenance: string;
  // status: string;
  // total_events: number;
  // total_req_events: number;
  // total_req_jobs: number;
  // total_done_jobs: number;
  // submit_time: string;
  // start_time: string;
  // timestamp: string;
  // pptimestamp: string;
  // postproduction: string;
  // priority: number;
  // current_priority: number;
  // update_time: string;
  // update_owner: string;
  // comments: string;
  // inputdataset: string;
  // physics_tag: string;
  // reference: string;
  // campaign: string;
  // jedi_info: string;
  // total_files_failed: number;
  total_files_tobeused: number;
  // total_files_used: number;
  // total_files_onhold: number;
  // is_extension: boolean;
  total_files_finished: number;
  // ttcr_timestamp: string;
  // ttcj_timestamp: string;
  // ttcj_update_time: string;
  // primary_input: string;
  // ami_tag: string;
  // output_formats: string;
  // step: any;
  // request: any;
}
