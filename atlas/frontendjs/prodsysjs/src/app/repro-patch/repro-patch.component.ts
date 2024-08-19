import {Component, inject, Input} from '@angular/core';
import {ReproPatchService} from "./repro-patch.service";
import {JsonPipe} from "@angular/common";

@Component({
  selector: 'app-repro-patch',
  standalone: true,
  imports: [
    JsonPipe
  ],
  templateUrl: './repro-patch.component.html',
  styleUrl: './repro-patch.component.css'
})
export class ReproPatchComponent {

  @Input() set requestID(value: string) {
    this.requestPageID = value;
    this.repoPatchService.setRequestID(value);
  }
  repoPatchService = inject(ReproPatchService);

  requestPageID: string;

  isLoading = this.repoPatchService.state.isLoading();
  error = this.repoPatchService.state.error();
  taskPatchData = this.repoPatchService.state.taskPatchData();


}
