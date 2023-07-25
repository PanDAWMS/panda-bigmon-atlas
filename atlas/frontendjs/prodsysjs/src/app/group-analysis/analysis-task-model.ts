import {ProductionTask, SliceBase, StepBase} from "../production-request/production-request-models";

export interface AnalysisStepBase {
  id: number;
  status: string;
  name: string;
  step_parameters: TaskTemplate;
  step_production_parent_id: number;
  step_analysis_parent_id: number;
  request_id: number;
  slice_id: number;
  template_name: string;

}

export const ANALISYS_SOURCE_ACTIONS = ['EL'];
export interface AnalysisStep {
  step: StepBase;
  analysis_step: AnalysisStepBase;
  tasks?: ProductionTask[];
}
export interface AnalysisSlice {
  slice: SliceBase;
  steps?: AnalysisStep[];
}
export interface JobParameter {
  type: string;
  value: string;
  hidden: boolean;
  param_type: string;
  dataset: string;
  container: string;
  exclude: string;
  expand: boolean;
  padding: boolean;
  include: string;
  offset: number;
  consolidate: string;
  nFilesPerJob: number;
  destination: string;
  token: string;
  eventRatio: number;
  ratio: number;
  transient: boolean;
  random: string;
}

export interface TaskLogTemplate {
  dataset: string;
  container: string;
  type: string;
  param_type: string;
  value: string;
  destination: string;
  offset: number;
  token: string;
  transient: boolean;
}

export interface BuildSpec {
  prodSourceLabel: string;
  archiveName: string;
  jobParameters: string;
}

export interface MergeSpec {
  useLocalIO: number;
  jobParameters: string;
}

export interface ESMergeSpec {
  jobParameters: string;
  transPath: string;
}

export interface MultiSpecExec {
  preprocess: {command: string; args: string; };
  postprocess: {command: string; args: string; };
  containerOptions: {containerExec: string; containerImage: string; };
}

export interface TaskTemplate {
  taskName: string;
  uniqueTaskName: boolean;
  vo: string;
  architecture: string;
  transUses: string;
  transHome: string;
  processingType: string;
  prodSourceLabel: string;
  site: string;
  excludedSite: [string];
  includedSite: string;
  cliParams: string;
  osInfo: string;
  nMaxFilesPerJob: number;
  respectSplitRule: boolean;
  sourceURL: string;
  log: Partial<TaskLogTemplate>;
  jobParameters: [Partial<JobParameter>];
  dsForIN: string;
  buildSpec: BuildSpec;
  mergeSpec: MergeSpec;
  mergeOutput: boolean;
  userName: string;
  taskType: string;
  taskPriority: number;
  nFilesPerJob: number;
  fixedSandbox: string;
  walltime: number;
  coreCount: number;
  noInput: boolean;
  nEvents: number;
  nEventsPerJob: number;
  noEmail: boolean;
  osMatching: boolean;
  useRealNumEvents: boolean;
  skipScout: boolean;
  official: boolean;
  respectLB: boolean;
  maxAttempt: number;
  useLocalIO: number;
  workingGroup: string;
  addNthFieldToLFN: string;
  getNumEventsInMetadata: boolean;
  baseRamCount: number;
  campaign: string;
  cloud: string;
  cpuTimeUnit: string;
  ipConnectivity: string;
  maxFailure: number;
  mergeCoreCount: number;
  nGBPerJob: number;
  noWaitParent: boolean;
  ramCount: number;
  ramUnit: string;
  reqID: number;
  scoutSuccessRate: number;
  ticketID: string;
  ticketSystemType: string;
  transPath: string;
  ramCountUnit: string;
  container_name: string;
  avoidVP: boolean;
  runOnInstant: boolean;
  nEventsPerFile: number;
  parentTaskName: string;
  nFiles: number;
  disableAutoFinish: boolean;
  failWhenGoalUnreached: boolean;
  goal: string;
  ttcrTimestamp: string;
  useExhausted: boolean;
  gshare: string;
  ioIntensity: number;
  ioIntensityUnit: string;
  maxWalltime: number;
  orderByLB: boolean;
  outDiskCount: number;
  outDiskUnit: string;
  tgtMaxOutputForNG: number;
  inputPreStaging: boolean;
  noThrottle: boolean;
  releasePerLB: boolean;
  toStaging: boolean;
  nGBPerMergeJob: string;
  nEventsPerInputFile: number;
  reuseSecOnDemand: boolean;
  allowInputLAN: string;
  cpuTime: number;
  disableReassign: boolean;
  esConvertible: boolean;
  maxEventsPerJob: number;
  minGranularity: number;
  nucleus: string;
  skipShortInput: boolean;
  baseWalltime: number;
  waitInput: number;
  esmergeSpec: ESMergeSpec;
  notDiscardEvents: boolean;
  taskBrokerOnMaster: boolean;
  noLoopingCheck: boolean;
  tgtNumEventsPerJob: number;
  countryGroup: string;
  disableAutoRetry: number;
  multiStepExec: MultiSpecExec;
}



export interface TemplateBase {
  id: number;
  tag: string;
  task_parameters: Partial<TaskTemplate>;
  variables: any;
  build_task: number;
  source_tar: string;
  source_action: string;
  description: string;
  timestamp: string;
  username: string;
  physics_group: string;
  software_release: string;
  status: string;
}
