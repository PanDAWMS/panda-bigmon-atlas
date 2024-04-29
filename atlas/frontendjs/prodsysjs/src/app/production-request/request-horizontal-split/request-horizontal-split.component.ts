import { Component } from '@angular/core';
import {ActivatedRoute} from "@angular/router";
import {ProductionRequestService} from "../production-request.service";
import {catchError, switchMap, tap} from "rxjs/operators";
import {AsyncPipe, JsonPipe, NgForOf} from "@angular/common";
import {MatFormFieldModule} from "@angular/material/form-field";
import {MatOptionModule} from "@angular/material/core";
import {MatSelectModule} from "@angular/material/select";
import {FormBuilder, FormsModule, ReactiveFormsModule} from "@angular/forms";
import {MatCheckboxModule} from "@angular/material/checkbox";
import {MatButtonModule} from "@angular/material/button";
import {throwError} from "rxjs";
import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";

@Component({
  selector: 'app-request-horizontal-split',
  standalone: true,
  imports: [
    AsyncPipe,
    JsonPipe,
    MatFormFieldModule,
    MatOptionModule,
    MatSelectModule,
    NgForOf,
    ReactiveFormsModule,
    FormsModule,
    MatCheckboxModule,
    MatButtonModule,
    MatProgressSpinnerModule
  ],
  templateUrl: './request-horizontal-split.component.html',
  styleUrl: './request-horizontal-split.component.css'
})
export class RequestHorizontalSplitComponent {

   constructor(private productionRequestService: ProductionRequestService , private route: ActivatedRoute,  private fb: FormBuilder) { }

    error: string | undefined;
    requestID: number| undefined;
    mcPatternsSelectedPatterns: number[] = [];
    mcPatternSteps: string[] = [];
    approveRequests = false;
    submitting = false;
    errorMessage: string | undefined;
    createdRequests: number[] = [];
    requestTransitions$ = this.route.paramMap.pipe(
      switchMap(params => {
        this.requestID = Number(params.get('requestID'));
        return this.productionRequestService.getSplitByCampaign(this.requestID.toString());
      }), tap((result) => {
        for (const transition of result.print_results){
          console.log(result.patterns);
          for (const action of transition.transitions){

            if (action === 'Apply pattern'){
                let patternID = 0;
                if (result.patterns[transition.name]){
                  patternID = result.patterns[transition.name];
                }
                this.mcPatternsSelectedPatterns.push(patternID);
                this.mcPatternSteps.push(transition.name);
            }
          }
        }
      }), catchError(err => {
        if (err.status === 500) {
          this.errorMessage = ` Error creating requests: ${err.error}`;
        } else {
          this.errorMessage = ` Error creating requests: ${err.error} (status ${err.status})`;
        }
        return throwError(err);
      }
    ));

  onApply(): void {
    const patterns = {};
    for (const pattern of this.mcPatternsSelectedPatterns){
      const patternName = this.mcPatternSteps[this.mcPatternsSelectedPatterns.indexOf(pattern)];
      patterns[patternName] = pattern;
    }
    this.submitting = true;
    this.errorMessage = '';
    this.productionRequestService.splitRequestHorizontaly(this.requestID.toString(), this.approveRequests, patterns).subscribe(
      (result) => {
        this.createdRequests = result;
        this.submitting = false;

      },
      (err) => {
        this.submitting = false;
        if (err.status === '500') {
          this.errorMessage = ` Error creating requests: ${err.error}`;
        } else {
          this.errorMessage = ` Error creating requests: ${err.error} (status ${err.status})`;
        }
      }
    );
  }
}
