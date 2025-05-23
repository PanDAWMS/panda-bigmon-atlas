import {Component, inject, input, output} from '@angular/core';
import {catchError, filter, switchMap, takeWhile, tap} from 'rxjs/operators';
import {interval, Observable, throwError} from 'rxjs';
import {toObservable} from '@angular/core/rxjs-interop';
import {AsyncProdTaskSplitStatus, ProductionRequestService} from "../../production-request/production-request.service";
import {MatProgressSpinner} from "@angular/material/progress-spinner";
import {MatProgressBar} from "@angular/material/progress-bar";

@Component({
  selector: 'app-async-task-progress',
  imports: [
    MatProgressSpinner,
    MatProgressBar
  ],
  templateUrl: './async-task-progress.component.html',
  styleUrl: './async-task-progress.component.css'
})
export class AsyncTaskProgressComponent {
  asyncTaskId = input('');
  interval = input(5); // Interval in seconds
  private productionRequestService = inject(ProductionRequestService);
  taskFinished = output<AsyncProdTaskSplitStatus>();
  taskStatus$: Observable<AsyncProdTaskSplitStatus>;
  active = false;
  total = 0;
  current = 0;
  constructor() {
    const asyncTaskId$ = toObservable(this.asyncTaskId);
    const interval$ = toObservable(this.interval);

    this.taskStatus$ = interval$.pipe(
      switchMap(intervalSeconds =>
        asyncTaskId$.pipe(
          filter(taskId => taskId !== ''), // Only proceed if asyncTaskId is not empty
          switchMap(taskId =>
            interval(intervalSeconds * 1000).pipe( // Convert seconds to milliseconds
              switchMap(() => this.productionRequestService.getAsyncTaskStatus(taskId)),
              tap(status => {
                this.active = true;
                if (status.status === 'SUCCESS' || status.status === 'FAILURE') {
                  this.active = false;

                  this.taskFinished.emit(status);
                } else {
                  if (status.progress){
                    if (status.progress.progress){
                      this.total = 100;
                      this.current = status.progress.progress;
                    }
                    else {
                        this.total = status.progress.total;
                        this.current = status.progress.processed;
                    }
                  }
                }
              }), catchError(err => {
                    let errorMessage = '';
                    this.active = false;
                    if (err.status === 500) {
                      errorMessage = ` Error creating requests: ${err.error}`;
                    } else {
                      errorMessage = ` Error creating requests: ${err.error} (status ${err.status})`;
                    }
                    const returnStatus = {status: 'FAILURE', result: errorMessage, progress: undefined};
                    this.taskFinished.emit(returnStatus);
                    throw new Error(errorMessage);
                  }),
              takeWhile(status => status.status !== 'SUCCESS' && status.status !== 'FAILURE' && status.status !== 'REVOKED', true)
            )
          )
        )
      )
    );
    this.taskStatus$.subscribe();
  }
}
