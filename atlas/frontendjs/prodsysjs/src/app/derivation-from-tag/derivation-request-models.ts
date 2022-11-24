import {ProductionRequestBase, ProductionRequests} from "../production-request/production-request-models";

export interface DerivationDatasetInfo {
  dataset: string;
  outputs: string;
  task_id: number;
  request_id: number;
  task_status: string;
}

export interface DerivationContainersInput {
  container: string;
  datasets: DerivationDatasetInfo[];
  is_wrong_name: boolean;
  is_failed: boolean;
  is_running: boolean;
  has_failing: boolean;
  requests_id: number[];
  output_formats: string[];
  projects: string[];
}

export interface DerivationContainersCollection {
  containers: DerivationContainersInput[];
  requests: ProductionRequestBase[];
  format_outputs: string[];
  projects: string[];
}
