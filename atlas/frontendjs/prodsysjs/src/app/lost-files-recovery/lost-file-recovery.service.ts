import {Injectable, signal} from '@angular/core';
import {httpResource} from "@angular/common/http";

interface FileRecoveryParameters {
  dry_run: boolean;
  reproduce_parent: boolean;
  no_child_retry: boolean;
  log_file: string;
  submitted: string;
}

interface FileRecoveryCache {
  async_task_id: string;
  dataset: string;
  parameters: FileRecoveryParameters;
}

interface DatasetLostFileInfo {
  dataset: string;
  lost_files: number;
  recoveryInfo: FileRecoveryCache | null;
  recreateParent: boolean;
}



type DatasetLostFileResponse = DatasetLostFileInfo;

@Injectable({
  providedIn: 'root'
})
export class LostFileRecoveryService {
  selectedTask = signal<string>('');
  datasetName = signal<string>('');
  constructor() { }

  lostFileRecoveryInfoResource = httpResource<DatasetLostFileResponse>(
    () => {
      if (this.datasetName()) {
        let url = `/api/dataset_lost_file_info/?dataset=${this.datasetName()}`;
        if (this.selectedTask()) {
          url += `&task_id=${this.selectedTask()}`;
        }
        return url;
      }
    }
  );
}
