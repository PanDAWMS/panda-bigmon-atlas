import {Component, OnDestroy, OnInit} from '@angular/core';
import {
  catchError,
  combineLatest,
  delay,
  distinctUntilChanged, filter,
  ignoreElements,
  shareReplay,
  switchMap,
  tap
} from 'rxjs/operators';
import {ActivatedRoute} from '@angular/router';
import {TaskActionResult, TaskService} from './task-service.service';
import {BehaviorSubject, EMPTY, Observable, of} from "rxjs";


@Component({
  selector: 'app-production-task',
  templateUrl: './production-task.component.html',
  styleUrls: ['./production-task.component.css']
})
export class ProductionTaskComponent implements OnInit, OnDestroy{

  public SYNC_ACTIONS = ['sync_jedi'];
  public syncActions$ = this.taskService.getActionResults().pipe(
      filter(value => value !== null),
      filter(value => this.SYNC_ACTIONS.indexOf(value.action) > -1),
      tap(_ => this.pageUpdate$.next(100))
    ).subscribe();
  public pageUpdate$ = new BehaviorSubject(0);
  // public task$ = this.route.paramMap.pipe(switchMap((params) => this.pageUpdate$.pipe(
  //   switchMap((value) => this.taskService.getTask(params.get('id')).pipe(delay(value),
  //     tap(_ => console.log('data taken'))))
  // )));

  public task$ = this.pageUpdate$.pipe(switchMap((value) => this.route.paramMap.pipe(
    switchMap((params) => this.taskService.getTask(params.get('id')).pipe(delay(value),
      tap(_ => console.log('data taken')))))));

  //   this.pageUpdate$.pipe(
  //   switchMap((delayValue) => delay(delayValue)));
  // //   this.route.paramMap.pipe(
  // //   switchMap((params) => this.taskService.getTask(params.get('id')))
  // // );

  // public actionLog$ = this.task$.pipe(switchMap((taskInfo) => this.taskService.getTaskActionLogs(taskInfo.task.id.toString())));
  public actionLog$ = this.route.paramMap.pipe(switchMap((params) =>
    this.taskService.getTaskActionLogs(params.get('id').toString())));
  constructor(private route: ActivatedRoute, private taskService: TaskService) {
  }

  ngOnInit(): void  {

  }

  ngOnDestroy(): void {
    this.syncActions$.unsubscribe();
  }


}
