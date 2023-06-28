import {Component, EventEmitter, Input, OnInit, Output} from '@angular/core';
import {AnalysisTasksService} from "../analysis-tasks.service";

@Component({
  selector: 'app-analy-request-actions',
  templateUrl: './analy-request-actions.component.html',
  styleUrls: ['./analy-request-actions.component.css']
})
export class AnalyRequestActionsComponent implements OnInit {
  @Input() selectedSlices: number[] = [];
  @Input() productionRequestID: string;
  @Output() updateRequest = new EventEmitter<boolean>();
  public sendMessage = '';
  constructor(private analysisTaskService: AnalysisTasksService) { }

  ngOnInit(): void {
  }

  executeAction(action: string): void {
    this.sendMessage = 'loading...';
    if (action === 'submit') {
      this.analysisTaskService.submitAnalysisRequestAction(this.productionRequestID, 'submit', this.selectedSlices).subscribe(
        (response) => {
          this.sendMessage = response.result;
          this.updateRequest.emit(true);
        }
      );
    }
  }
}
