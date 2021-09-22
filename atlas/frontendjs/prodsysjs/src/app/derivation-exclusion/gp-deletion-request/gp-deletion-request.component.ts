import { Component, OnInit } from '@angular/core';
import {FormControl} from "@angular/forms";
import {MatTableDataSource} from "@angular/material/table";
import {DeletionSubmission} from "../gp-deletion-container";
import {GpDeletionRequestService} from "./gp-deletion-request.service";
import {MatDatepickerInputEvent} from "@angular/material/datepicker";

@Component({
  selector: 'app-gp-deletion-request',
  templateUrl: './gp-deletion-request.component.html',
  styleUrls: ['./gp-deletion-request.component.css']
})
export class GpDeletionRequestComponent implements OnInit {

  deadlineDate = new FormControl(new Date());
  startDeletionDate = new FormControl(new Date());
  DeletionRequestsDataSource: MatTableDataSource<DeletionSubmission>;
  submissionDisabled = true;
  currentRequest: DeletionSubmission|undefined = undefined;
  minDate: Date = new Date();
  tomorrow: Date = new Date();

  constructor(private gpDeletionRequestService: GpDeletionRequestService) { }

  ngOnInit(): void {
    this.tomorrow.setDate(this.tomorrow.getDate() + 1);
    this.minDate = this.tomorrow;
    this.startDeletionDate.setValue(this.minDate);

    this.updateDeletionRequestTable(true);
    }

  changeDeadline(event: MatDatepickerInputEvent<Date>): void {
    if (event.value > this.tomorrow){
      this.minDate =  event.value;
    } else {
      this.minDate = this.tomorrow;
    }
    if (this.startDeletionDate.value  < this.minDate) {
      this.startDeletionDate.setValue(this.minDate);
    }
  }

  submitDeletionRequest(): void {
    this.submissionDisabled = true;
    this.gpDeletionRequestService.postDeletionRequests(this.deadlineDate.value, this.startDeletionDate.value).subscribe( newRequest => {
      this.currentRequest = newRequest;
      if (this.currentRequest === undefined){
        this.submissionDisabled = false;
      }
      this.updateDeletionRequestTable(false);
    });
  }

    updateDeletionRequestTable(checkCurrent: boolean): void{
      this.DeletionRequestsDataSource = new MatTableDataSource<DeletionSubmission>();
      this.gpDeletionRequestService.getExistingDeletionRequests().subscribe(deletionRequests => {
        this.DeletionRequestsDataSource.data = deletionRequests;
        if (checkCurrent){
          for (const deletionRequest of deletionRequests){
            if (  ['Waiting', 'Submitted', 'Executing'].indexOf(deletionRequest.status) > -1){
              this.currentRequest = deletionRequest;
              break;
            }
          }
          if (this.currentRequest === undefined){
            this.submissionDisabled = false;
          }
        }
      });
    }


}
