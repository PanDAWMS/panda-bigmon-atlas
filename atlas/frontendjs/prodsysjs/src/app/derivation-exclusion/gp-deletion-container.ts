export interface GroupProductionDeletionContainer {
    id: number;
    container: string;
    dsid: number;
    output_format: string;
    update_time: string;
    version: number;
    status: string;
    size: number;
    skim: string;
    input_key: string;
    datasets_number: number;
    events: number;
    available_tags: string;
    extensions_number: number;
    last_extension_time: string;
    ami_tag: string;
    previous_container: any;
    age?: number;
    epoch_last_update_time?: number;
    is_expired?: string;
    expended_till?: number;
}

export interface ExtensionRequest {
  containers: GroupProductionDeletionContainer[];
  message: string;
  number_of_extensions: number;
}

export interface DeletionSubmission {
  id: number;
  deadline: string;
  start_deletion: string;
  username: string;
  status: string;
  containers?: number;
  size?: number;
}
