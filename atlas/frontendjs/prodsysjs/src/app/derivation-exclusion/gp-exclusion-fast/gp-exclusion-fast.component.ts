import {Component, inject, input, OnInit} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {GroupProductionDeletionContainer} from "../gp-deletion-container";
import {GPDeletionContainerService} from "../gp-deleation.service";
import {combineLatest} from "rxjs";
import {toObservable} from "@angular/core/rxjs-interop";
import {AgGridAngular} from "ag-grid-angular";
import {ColDef, FirstDataRenderedEvent, RowDataUpdatedEvent, SelectionChangedEvent} from "ag-grid-community";
import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";
import {MatFormFieldModule} from "@angular/material/form-field";
import {MatInputModule} from "@angular/material/input";
import {MatButtonModule} from "@angular/material/button";

@Component({
  selector: 'app-gp-exclusion-fast',
  imports: [
    AgGridAngular,
    FormsModule,
    MatProgressSpinnerModule,
    MatFormFieldModule,
    MatInputModule,
    MatButtonModule,
  ],
  templateUrl: './gp-exclusion-fast.component.html',
  styleUrl: './gp-exclusion-fast.component.css',
})
export class GpExclusionFastComponent implements OnInit {
    amiTag = input<string>('amiTag');
    outputType = input<string>('outputType');
    dataType = input<string>('dataType');
    gpDeletionContainerService = inject(GPDeletionContainerService);

    containers: GroupProductionDeletionContainer[] = [];
    selectedContainers: GroupProductionDeletionContainer[] = [];
    isLoading = false;
    isSubmittingExtension = false;
    numberOfExtensions: number | null = 1;
    comment = '';
    extensionRequestResult = '';
    extensionRequestError = '';

    columnDefs: ColDef<GroupProductionDeletionContainer>[] = [
      {
        headerName: '',
        checkboxSelection: true,
        headerCheckboxSelection: true,
        width: 52,
        maxWidth: 52,
        pinned: 'left',
        sortable: false,
        filter: false,
        resizable: false,
      },
      {field: 'container', headerName: 'Container', filter: 'agTextColumnFilter',
      cellRenderer: (params) => {
        return `<a href="/ng/gp-container-details/${params.value}">${params.value}</a>`
      }
      },
      {field: 'available_tags', headerName: 'Available tags', filter: 'agTextColumnFilter'},
      {
        field: 'age',
        headerName: 'Age',
        cellRenderer: (params) => {
          const rawValue = params.value;
          if (rawValue === null || rawValue === undefined || rawValue === '') {
            return '';
          }
          const value = String(Math.trunc(Number(rawValue)));
          const status = params.data?.is_expired;
          const backgroundColor = status === 'expired' ? 'red' : status === 'extension' ? 'green' : 'orange';
          return `<span style="background-color:${backgroundColor};display:inline-block;min-width:2rem;padding:0.125rem 0.5rem;border-radius:0.25rem;text-align:center;color:white;">${value}</span>`;
        },
      },
      {field: 'extensions_number', headerName: 'Extensions'},
      {
        field: 'expended_till',
        headerName: 'Expended till',
        valueFormatter: (params) => params.value ? new Date(Number(params.value)).toLocaleString() : '',
      },
    ];

    constructor() {
      combineLatest([
            toObservable(this.amiTag),
            toObservable(this.outputType),
            toObservable(this.dataType),
        ]).subscribe(() => {
            this.loadContainers(true);
        });
    }

    private loadContainers(clearMessages = false): void {
      this.isLoading = true;
      this.selectedContainers = [];
      if (clearMessages) {
        this.extensionRequestResult = '';
        this.extensionRequestError = '';
      }

      this.gpDeletionContainerService.getGPDeletionPerOutputTag(
        this.outputType(),
        this.dataType(),
        this.amiTag(),
      ).subscribe((containers) => {
        this.containers = containers;
        this.isLoading = false;
      });
    }

    autoSizeColumns(event: FirstDataRenderedEvent | RowDataUpdatedEvent): void {
      event.api.autoSizeAllColumns();
    }

    onSelectionChanged(event: SelectionChangedEvent<GroupProductionDeletionContainer>): void {
      this.selectedContainers = event.api.getSelectedRows();
    }

    askForExtension(selectedContainers: GroupProductionDeletionContainer[], numberOfExtensions: number | null, comment: string): void {
      this.extensionRequestResult = '';
      this.extensionRequestError = '';

      const trimmedComment = comment.trim();
      if (selectedContainers.length === 0) {
        this.extensionRequestError = 'Select at least one container.';
        return;
      }

      if (!trimmedComment) {
        this.extensionRequestError = 'Comment is required.';
        return;
      }

      this.isSubmittingExtension = true;
      this.gpDeletionContainerService.askExtension({
        containers: selectedContainers,
        message: trimmedComment,
        number_of_extensions: numberOfExtensions,
      }).subscribe({
        next: (result) => {
          this.isSubmittingExtension = false;
          if (result) {
            this.extensionRequestResult = `Extension request submitted for ${selectedContainers.length} container(s).`;
            this.comment = '';
            this.loadContainers(false);
          } else {
            this.extensionRequestError = 'Extension request failed. No response was returned.';
          }
        },
        error: (error) => {
          this.isSubmittingExtension = false;
          this.extensionRequestError = error?.error?.message || error?.message || 'Extension request failed.';
        }
      });
    }

    ngOnInit(): void {

    }
}
