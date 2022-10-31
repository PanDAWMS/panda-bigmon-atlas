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

  public inputEvents: number|null = null;
  public inputSize: number|null = null;

  public task$ = this.pageUpdate$.pipe(switchMap((value) => this.route.paramMap.pipe(
    switchMap((params) => this.taskService.getTask(params.get('id')).pipe(delay(value))))));

  public taskStats$ = this.route.paramMap.pipe(switchMap((params) =>
    this.taskService.getTaskStats(params.get('id').toString())),
    tap( stat => {
      this.inputEvents = stat.input_events;
      this.inputSize = stat.input_bytes; }));

  public actionLog$ = this.route.paramMap.pipe(switchMap((params) =>
    this.taskService.getTaskActionLogs(params.get('id').toString())));

  public JEDIErrorLog$ = this.route.paramMap.pipe(switchMap((params) =>
    this.taskService.getTaskErrorLogs(params.get('id').toString())));

  public taskExtensions$ = this.route.paramMap.pipe(switchMap((params) =>
    this.taskService.getTaskExtension(params.get('id').toString())));

  constructor(public route: ActivatedRoute, private taskService: TaskService) {
  }

  ngOnInit(): void  {

  }

  ngOnDestroy(): void {
    this.syncActions$.unsubscribe();
  }


}
