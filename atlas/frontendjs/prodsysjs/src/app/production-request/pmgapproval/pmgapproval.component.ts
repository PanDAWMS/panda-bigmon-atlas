import {Component, computed, inject, input, OnInit, signal} from '@angular/core';
import {ProductionRequestService} from "../production-request.service";
import {setErrorMessage} from "../../dsid-info/dsid-info.service";
import {HttpErrorResponse} from "@angular/common/http";
import {MatProgressSpinner} from "@angular/material/progress-spinner";
import {NgForOf} from "@angular/common";
import {toObservable} from "@angular/core/rxjs-interop";
import {MatTab, MatTabGroup} from "@angular/material/tabs";
import {
  MatAccordion,
  MatExpansionPanel, MatExpansionPanelDescription,
  MatExpansionPanelHeader,
  MatExpansionPanelTitle
} from "@angular/material/expansion";
import {Router} from "@angular/router";
import {MatButton} from "@angular/material/button";

interface CheckError {
  name: string;
  message: string;
  type: 'REQUEST' | 'SLICE';
  positions: {
      RequestIDs: number;
      sliceIDs: number;
  }[];
}
interface CheckSummary {
  errors: CheckError[];
  status: 'ERROR' | 'WARNING' | 'PASSED';
}
@Component({
  selector: 'app-pmgapproval',
  imports: [
    MatProgressSpinner,
    MatTabGroup,
    MatTab,
    NgForOf,
    MatAccordion,
    MatExpansionPanel,
    MatExpansionPanelHeader,
    MatExpansionPanelTitle,
    MatExpansionPanelDescription,
    MatButton
  ],
  templateUrl: './pmgapproval.component.html',
  styleUrl: './pmgapproval.component.css'
})
export class PMGApprovalComponent implements OnInit{

    ERROR_CHECKUP_LIST = [
      'Energy check',
      'Project check',
      'Missing Job Option',
      'Not dividable job option',
      'Input events not dividable by 10k',
      'TID instead of container',
      'Not enough input',
      'Bad SW release',
      'AF2 in Run3',
      'Too many input files',
      'Too many jobs',
      'Wrong grid pack',
      'Total input events ratio'
    ];
    jira = input<string>('');
    jiraTicket$ = toObservable(this.jira);
    approveError = signal('');
    approveOnGoing = signal(false);
    productionRequestService = inject(ProductionRequestService);
    pmgApproval = this.productionRequestService.getPMGCheckUPResource.value;
    pmgCheckUP = computed(() => {
        const pmgCheckSummary = new Map<string, CheckSummary>();
        for (const checkName of this.ERROR_CHECKUP_LIST) {
            pmgCheckSummary.set(checkName, {errors: [], status: 'PASSED'});
        }
        if (this.pmgApproval()) {
            for (const check of this.pmgApproval().checks){
                let status: "ERROR" | "WARNING" | "PASSED" = 'WARNING';
                const currentErrors = pmgCheckSummary.get(check.check_name).errors || [];
                currentErrors.push({
                        name: check.check_name,
                        message: check.message || '',
                        type: check.type as 'REQUEST' | 'SLICE',
                        positions: check.step_position.map(pos => {
                            return {
                                RequestIDs: pos.request_id ,
                                sliceIDs: pos.slice_number
                            };
                        })

                    });
                if (check.status === 'ERROR') {
                  status = 'ERROR';
                }
                pmgCheckSummary.set(check.check_name, {errors: currentErrors, status} );
            }
        } else {
            return undefined;
        }
        // convert the Map to an object to the list
        return Array.from(pmgCheckSummary.entries()).map(([key, value]) => {
          return {
            check_name: key,
            errors: value.errors,
            status: value.status
          };
        });
    });
    isLoading = this.productionRequestService.getPMGCheckUPResource.isLoading;
    error = this.productionRequestService.getPMGCheckUPResource.error;
    errorMessage = computed(() => {
        if (this.error()) {
            return setErrorMessage(this.error() as HttpErrorResponse | null);
        } else {
            return null;
        }
    });

    approvePMGCheckUp(status: string): void {
        if (this.jira()) {
            this.approveOnGoing.set(true);
            this.approveError.set('');
            this.productionRequestService.setNewPMGStatus(this.jira(), status).subscribe({
                next: (result) => {
                    window.location.href ='/prodtask/inputlist_with_request/'+result.requestIDs[0];
                },
                error: (err) => {
                    this.approveError.set('Error approving PMG Check Up : ' + setErrorMessage(err));
                }
            });
        } else {
            console.error('JIRA ticket is required to approve PMG Check Up');
        }
    }
    constructor(private router: Router) {
    }
    ngOnInit(): void {
        this.jiraTicket$.subscribe( jiraTicket => {
            if (jiraTicket) {
                this.productionRequestService.jiraForPMGCheckUp.set(jiraTicket);
            }
        });
    }

}
