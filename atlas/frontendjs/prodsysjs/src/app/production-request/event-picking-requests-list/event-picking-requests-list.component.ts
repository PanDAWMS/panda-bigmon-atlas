import {Component, inject, OnInit, signal} from '@angular/core';
import {EPRequestShort, EventPickingService} from "../event-picking-request-creation/event-picking.service";
import {setErrorMessage} from "../../dsid-info/dsid-info.service";
import {AgGridAngular} from "ag-grid-angular";
import {RouterLink} from "@angular/router";

@Component({
  selector: 'app-event-picking-requests-list',
  imports: [
    AgGridAngular,
    RouterLink
  ],
  templateUrl: './event-picking-requests-list.component.html',
  styleUrl: './event-picking-requests-list.component.css'
})
export class EventPickingRequestsListComponent implements OnInit{

  epService = inject(EventPickingService);
  epRequests: EPRequestShort[] = [];
  loading = signal(false);
  error = signal('');
  protected epRequestsColumns = [
    {
      field: 'jira',
      headerName: 'jira',
      cellRenderer: params => {
        const jira = params.value.split('/').pop() ?? '';
        return `<a href="/ng/event-picking-request/${jira}">${jira}</a>`;
      }

      },
      {
      field: 'description',
      headerName: 'description',
      },
    {
      field: 'requestor',
      headerName: 'requestor',
    }
  ];

  ngOnInit(): void {
    this.loading.set(true);
    this.epService.getEPRequestsList().subscribe({
        next: (response) => {
          this.epRequests = response;
          this.loading.set(false);
        },
        error: (err) => {
          this.error.set(setErrorMessage(err));
        }
      });
  }

}
