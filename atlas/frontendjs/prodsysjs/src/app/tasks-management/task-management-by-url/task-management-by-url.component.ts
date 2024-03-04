import {Component, OnInit} from '@angular/core';
import {ActivatedRoute, DefaultUrlSerializer, Router} from "@angular/router";
import {TasksManagementService} from "../tasks-management.service";
import {HttpParams} from "@angular/common/http";
import {catchError, switchMap, tap} from "rxjs/operators";
import {Observable, of} from "rxjs";
import {ProductionTask} from "../../production-request/production-request-models";

@Component({
  selector: 'app-task-management-by-url',
  templateUrl: './task-management-by-url.component.html',
  styleUrl: './task-management-by-url.component.css'
})
export class TaskManagementByUrlComponent implements OnInit{
  bigpandaURL$ = this.route.queryParams.pipe(switchMap((params) => {
    this.httpParams = new HttpParams({fromObject: params});
    if (this.httpParams.toString() === ''){
      return of('');
    }
    return of('https://bigpanda.cern.ch/tasks/?' + this.httpParams.toString());
  }
  ));
  public loadError?: string;
  loading = false;
  tasks$: Observable<ProductionTask[]>;
  bigpandaURL: string;
  httpParams = new HttpParams();
   public urlSerializer = new DefaultUrlSerializer();

  constructor(public route: ActivatedRoute, public router: Router, public taskManagementService: TasksManagementService) { }

 ngOnInit(): void {
    this.bigpandaURL$.subscribe((url) => {
      this.bigpandaURL = url;
    });
 }

  getTasks(): void {
    const parameterStr = this.bigpandaURL.split('?')[1];
    const httpParams = this.urlSerializer.parse('./?' + parameterStr);
    this.router.navigate(['.' ],
        { queryParams: httpParams.queryParams, relativeTo: this.route });
    this.loading = true;
    this.loadError = undefined;
    this.tasks$ = this.taskManagementService.getTasksByBigpandaUrl(this.bigpandaURL).pipe(
        tap(() => this.loading = false),
       catchError((err) => {
        this.loading = false;
        if (err.status !== '400') {
          this.loadError = `Error task loading ${err.error}`;
        } else {
          this.loadError = `Error task loading ${err.error} (status ${err.status})`;
        }
        return of([] as ProductionTask[]);
    }));
  }

}
