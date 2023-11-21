import {Component, Input, OnInit, OnChanges} from '@angular/core';
import {ActivatedRoute} from '@angular/router';
import {TaskActionLog, TaskHS06, TaskInfo, TaskService} from '../../production-task/task-service.service';
import {ProductionTask} from '../production-request-models';
import {delay, filter, switchMap, tap} from 'rxjs/operators';
import {BehaviorSubject, Observable} from 'rxjs';
import {DEFAULTS} from "../../common/constants/tasks_constants";

@Component({
  selector: 'app-task-full-details',
  templateUrl: './task-details.component.html',
  styleUrls: ['./task-details.component.css']
})
export class TaskDetailsComponent implements OnInit, OnChanges {
  @Input() taskID: number;

  public SYNC_ACTIONS = ['sync_jedi'];
  public syncActions$ = this.taskService.getActionResults().pipe(
      filter(value => value !== null),
      filter(value => this.SYNC_ACTIONS.indexOf(value.action) > -1),
      tap(_ => this.pageUpdate$.next(100))
    ).subscribe();
  public pageUpdate$ = new BehaviorSubject(0);

  public inputEvents: number|null = null;
  public inputSize: number|null = null;
  public currentTask?: ProductionTask;

  public task$: Observable<TaskInfo>;

  public taskStats$: Observable<TaskHS06>;
  public actionLog$: Observable<TaskActionLog[]>;

  public JEDIErrorLog$: Observable<{log: string}>;

  public taskExtensions$: Observable<{id: number, status: string}[] | []>;
  constructor(public route: ActivatedRoute, private taskService: TaskService) { }

  ngOnChanges(): void {
    this.pageUpdate$.next(0);
  }
  ngOnInit(): void {
    this.task$ = this.pageUpdate$.pipe(switchMap((value) => this.taskService.getTask(this.taskID.toString()).pipe(delay(value))),
    tap(task => this.currentTask = {...task.task}));
    this.taskStats$ = this.taskService.getTaskStats(this.taskID.toString()).pipe(
    tap( stat => {
      this.inputEvents = stat.input_events;
      this.inputSize = stat.input_bytes; }));
    this.actionLog$ = this.taskService.getTaskActionLogs(this.taskID.toString().toString());
    this.JEDIErrorLog$ = this.taskService.getTaskErrorLogs(this.taskID.toString().toString());
    this.taskExtensions$ = this.taskService.getTaskExtension(this.taskID.toString().toString());
  }

  protected readonly DEFAULTS = DEFAULTS;
}
