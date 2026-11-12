import {Component, inject} from '@angular/core';
import {CommonModule} from '@angular/common';
import {MAT_DIALOG_DATA, MatDialogModule} from '@angular/material/dialog';
import {RouterLink} from '@angular/router';
import {catchError, of, tap} from 'rxjs';
import {DkbSearchService} from './dkb-search.service';
import {taskStatusStyle} from './dkb-task-status';

@Component({
  selector: 'app-dkb-task-details',
  imports: [CommonModule, MatDialogModule, RouterLink],
  templateUrl: './dkb-task-details.component.html',
  styleUrl: './dkb-task-details.component.css',
})
export class DkbTaskDetailsComponent {
  readonly taskID = inject<number>(MAT_DIALOG_DATA);
  private service = inject(DkbSearchService);
  loading = true;
  error = '';
  readonly statusStyle = taskStatusStyle;
  readonly groups = [
    {title: 'Task', fields: [
      ['taskid', 'Task ID'], ['taskname', 'Task name'], ['pr_id', 'Request'], ['status', 'Status'],
      ['description', 'Description'], ['user_name', 'User'], ['task_timestamp', 'Timestamp'],
      ['start_time', 'Start time'], ['end_time', 'End time'], ['hs06', 'HS06 per event'],
      ['output_formats', 'Outputs'], ['requested_events', 'Requested events'],
      ['processed_events', 'Processed events'], ['total_events', 'Total events'],
    ]},
    {title: 'Experiment', fields: [
      ['ctag', 'AMI tag'], ['campaign', 'Campaign'], ['subcampaign', 'Subcampaign'],
      ['project', 'Project'], ['energy_gev', 'Energy (GeV)'], ['phys_group', 'Physics group'],
      ['phys_category', 'Physics category'], ['hashtag_list', 'Hashtags'],
    ]},
    {title: 'Configuration', fields: [
      ['step_name', 'Step name'], ['ticket_id', 'Ticket ID'], ['architecture', 'Architecture'],
      ['core_count', 'Core count'], ['geometry_version', 'ATLAS geometry'],
      ['conditions_tag', 'Conditions tag'], ['trigger_config', 'Trigger configuration'],
      ['trans_path', 'Transform path'], ['trans_home', 'Transform home'], ['trans_uses', 'Transform uses'],
      ['run_number', 'Run number'],
    ]},
  ];
  readonly task$ = this.service.task(this.taskID).pipe(
    tap(() => this.loading = false),
    catchError(error => {
      this.loading = false;
      this.error = error?.error?.detail ?? 'Could not load the task from DKB.';
      return of(null);
    }),
  );

  display(value: unknown): string {
    if (value == null || value === '') { return '—'; }
    if (typeof value === 'number') { return value.toLocaleString(); }
    if (Array.isArray(value)) { return value.map(item => this.display(item)).join(', '); }
    return typeof value === 'object' ? JSON.stringify(value) : String(value);
  }

  datasetNames(value: string | string[] | null | undefined): string[] {
    return (Array.isArray(value) ? value : [value]).filter(name => typeof name === 'string' && name.trim() !== '');
  }
}
