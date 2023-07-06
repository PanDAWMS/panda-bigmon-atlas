import {Component, Inject, OnInit} from '@angular/core';
import {AnalysisTasksService} from '../analysis-tasks.service';
import {BehaviorSubject} from 'rxjs';
import {map, switchMap, tap} from 'rxjs/operators';
import {TemplateBase} from '../analysis-task-model';
import {ActivatedRoute, Router} from "@angular/router";
import {APP_BASE_HREF} from "@angular/common";

@Component({
  selector: 'app-analysis-templates-table',
  templateUrl: './analysis-templates-table.component.html',
  styleUrls: ['./analysis-templates-table.component.css']
})
export class AnalysisTemplatesTableComponent implements OnInit {
  public statusFilter = 'ACTIVE';
  public currentTemplates: TemplateBase[] = [];
  public filterChanged$  = new BehaviorSubject<boolean>(false);
  public allTemplates$ = this.filterChanged$.pipe(
    switchMap(_ => this.analysisTaskService.getAllActiveTemplates(this.statusFilter)),
    tap(templates => this.currentTemplates = templates));
  templatesAGColumns = [
    {
      headerName: 'Tag',
      field: 'tag',
      sortable: true,
      filter: true,
       maxWidth: 90,
      cellRenderer:  params => {
        return `<a href="${this.router.createUrlTree([this.baseHref, 'analysis-pattern', params.value])}" >${params.value}</a>`;
      },
    },
    {
      headerName: 'Description',
      field: 'description',
      sortable: true,
      filter: true,
      minWidth: 500,
      wrapText: true,
      autoHeight: true,
    },
    {
      headerName: 'Status',
      field: 'status',
             maxWidth: 120,

    },
    {
      headerName: 'Create Request',
      field: 'tag',
      cellRenderer:  params => {
        return `<a href="${this.router.createUrlTree([this.baseHref, 'create-analysis-request', params.value])}" >Create</a>`;
      },
      maxWidth: 140,
    }];



  constructor(private analysisTaskService: AnalysisTasksService, private router: Router, private route: ActivatedRoute,  @Inject(APP_BASE_HREF) private baseHref: string) { }

  ngOnInit(): void {
  }

}
