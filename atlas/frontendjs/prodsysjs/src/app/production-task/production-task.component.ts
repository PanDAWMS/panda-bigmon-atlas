import { Component, OnInit } from '@angular/core';
import {catchError, ignoreElements, switchMap} from 'rxjs/operators';
import {ActivatedRoute} from '@angular/router';
import {TaskService} from './task-service.service';
import {of} from "rxjs";

@Component({
  selector: 'app-production-task',
  templateUrl: './production-task.component.html',
  styleUrls: ['./production-task.component.css']
})
export class ProductionTaskComponent implements OnInit{

  public task$ = this.route.paramMap.pipe(
    switchMap((params) => this.taskService.getTask(params.get('id')))
  );
  public taskError$ = this.task$.pipe(ignoreElements(),
    catchError((err) => of(err)));
  public actionLog$ = this.task$.pipe(switchMap((taskInfo) => this.taskService.getTaskActionLogs(taskInfo.task.id.toString())));
  constructor(private route: ActivatedRoute, private taskService: TaskService) {
  }

  ngOnInit(): void  {
  }


}
