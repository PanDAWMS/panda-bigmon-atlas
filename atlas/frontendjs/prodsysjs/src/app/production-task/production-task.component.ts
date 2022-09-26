import { Component, OnInit } from '@angular/core';
import {switchMap} from "rxjs/operators";
import {ActivatedRoute} from "@angular/router";
import {TaskService} from "./task-service.service";

@Component({
  selector: 'app-production-task',
  templateUrl: './production-task.component.html',
  styleUrls: ['./production-task.component.css']
})
export class ProductionTaskComponent{

  public task$ = this.route.paramMap.pipe(
    switchMap((params) => this.taskService.getTask(params.get('id')))
  );
  public actionLog$ = this.task$.pipe(switchMap((task) => this.taskService.getTaskActionLogs(task.id.toString())))
  constructor(private route: ActivatedRoute, private taskService: TaskService) {
  }



}
