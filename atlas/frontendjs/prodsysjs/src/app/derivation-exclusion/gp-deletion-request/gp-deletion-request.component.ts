import {Component, OnInit, ViewChild} from '@angular/core';
import {UntypedFormControl} from "@angular/forms";
import {MatLegacyTableDataSource as MatTableDataSource} from "@angular/material/legacy-table";
import {DeletionSubmission} from "../gp-deletion-container";
import {DeletedContainers, GpDeletionRequestService} from "./gp-deletion-request.service";
import {MatDatepickerInputEvent} from "@angular/material/datepicker";
import {MatLegacyPaginator as MatPaginator} from "@angular/material/legacy-paginator";
import {MatSort} from "@angular/material/sort";




@Component({
  selector: 'app-gp-deletion-request',
  templateUrl: './gp-deletion-request.component.html',
  styleUrls: ['./gp-deletion-request.component.css']
})
export class GpDeletionRequestComponent implements OnInit {

  @ViewChild(MatPaginator) paginator: MatPaginator;
  @ViewChild(MatSort) sort: MatSort;
  deadlineDate = new UntypedFormControl(new Date());
  startDeletionDate = new UntypedFormControl(new Date());
  DeletionRequestsDataSource: MatTableDataSource<DeletionSubmission>;
  DeletionContainersDataSource: MatTableDataSource<DeletedContainers> = new  MatTableDataSource<DeletedContainers>();
  allContainersLoading = false;
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


  showContainers(): void {
    this.DeletionContainersDataSource.paginator = this.paginator;
    this.DeletionContainersDataSource.sort = this.sort;
    this.allContainersLoading = true;
    this.gpDeletionRequestService.getAllDeletedContainers().subscribe(result => {
      this.DeletionContainersDataSource.data = result;
      this.allContainersLoading = false;
    });
  }

  applyFilter(event: Event): void {
    const filterValue = (event.target as HTMLInputElement).value;
    this.DeletionContainersDataSource.filter = filterValue.trim().toLowerCase();
  }
}
