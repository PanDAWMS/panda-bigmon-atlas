import { Component, OnInit } from '@angular/core';
import {ActivatedRoute, Router} from "@angular/router";
import {TasksManagementService} from "./tasks-management.service";
import {catchError, switchMap} from "rxjs/operators";
import {of} from "rxjs";

@Component({
  selector: 'app-tasks-management',
  templateUrl: './tasks-management.component.html',
  styleUrls: ['./tasks-management.component.css']
})
export class TasksManagementComponent implements OnInit {

  constructor(private route: ActivatedRoute, private router: Router, private taskManagementService: TasksManagementService) { }
  public requestID = '';
  public taskID: string|null = null;
  public showOwner = false;
  public slices: number[] = [];
  public hashtagString = '';
  public loadError?: string;
  public displayedColumns = [ 'id', 'status', 'name', 'username', 'request_id', 'priority', 'total_events', 'failureRate', 'step_name', 'ami_tag'];
  public tasks$ =  this.route.paramMap.pipe(switchMap((params) => {
    if (params.get('hashtagString') ) {
      this.hashtagString = params.get('hashtagString').toString();
      this.showOwner = true;
      return this.taskManagementService.getTasksByHashtag(this.hashtagString);
    }
    this.requestID = params.get('id').toString();

    if (params.get('slices')){
      this.slices = this.convertSliceString(params.get('slices'));
      return this.taskManagementService.getTasksByRequestSlices(params.get('id'), this.slices);
    }
    return this.taskManagementService.getTasksByRequestSlices(params.get('id'), null);
  }), catchError((err) => {
    this.loadError = err.toString();
    return of([]);
  }));
  requestInfo$ = this.route.paramMap.pipe(switchMap((params) => {
    this.requestID = params.get('id').toString();
    return this.taskManagementService.getProductionRequest(params.get('id'));
  }));

  ngOnInit(): void {
    this.route.queryParamMap.subscribe(params => {
      this.taskID = params.get('task');
      console.log(this.taskID);
    });
  }

  onTaskChosen(taskID: number): void {
    this.router.navigate(['.'],
        { queryParams: {task: taskID}, queryParamsHandling: 'merge' , relativeTo: this.route });
  }
  convertSliceString(rangeStr: string): number[] {

      let token = '';
      const slices: number[] = [];
      let chainStart = -1;
      for (const ch of [...rangeStr]) {
        if (ch !== 'x' && ch !== 'y') {
          token += ch;
        } else {
          const currentValue = parseInt(token, 16);
          token = '';
          if (ch === 'x') {
            if (chainStart !== -1) {
              throw new Error('Wrong sequence to convert');
            }
            chainStart = currentValue;
          }
          if (ch === 'y') {
            if (chainStart !== -1) {
              slices.push(...Array.from(Array(currentValue + 1).keys()).slice(chainStart));
              chainStart = -1;
            } else {
              slices.push(currentValue);
            }
          }
        }
      }
      return slices;
    }
}