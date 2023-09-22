import { Component, OnInit } from '@angular/core';
import {ActivatedRoute} from "@angular/router";
import {AnalysisTasksService} from "../analysis-tasks.service";
import {catchError, switchMap, tap} from "rxjs/operators";
import {of} from "rxjs";
import {editState, PatternChanges} from "../pattern-edit/pattern-edit.component";
import {FormBuilder} from "@angular/forms";

@Component({
  selector: 'app-analysis-pattern',
  templateUrl: './analysis-pattern.component.html',
  styleUrls: ['./analysis-pattern.component.css']
})
export class AnalysisPatternComponent implements OnInit {

  public templateID: string;
  public errorMessage = '';
  public taskTemplate$  = this.route.paramMap.pipe(switchMap((params) => {
    this.templateID = params.get('tag');
    return this.analysisTaskService.getTemplate(params.get('tag'));
  }), tap((taskTemplate) => {this.templateForm.patchValue({status: taskTemplate.status}); }));
  public templateForm = this.fb.group({status: ['']});
  public editMode: editState = 'view';
  public loading = false;
  public expertView = 'Loading...';
  constructor(private route: ActivatedRoute, private analysisTaskService: AnalysisTasksService, private fb: FormBuilder) { }

  ngOnInit(): void {
     this.expertView = 'Loading...';
  }
  savePattern(data: PatternChanges): void {
    this.savePatternAndTemplate(data, true);
  }
  savePatternAndTemplate(data: PatternChanges, changeEditMode: boolean): void {
    if (changeEditMode) {
            this.editMode = 'disabled';
    }
    this.errorMessage = '';
    this.loading = true;
    this.analysisTaskService.saveTemplateParams(this.templateID, this.templateForm.value, data).pipe(catchError((err) => {
            if (changeEditMode) {
              this.editMode = 'edit';
            }
            this.errorMessage = err?.error;
            if (!this.errorMessage) {
              this.errorMessage = err.toString();
            }
            return of(err);
          })
    ).subscribe((result) => {
      this.loading = false;
    });
  }

  openExpertView(): void {
    this.expertView = 'Loading...';
    this.analysisTaskService.getAnalysisPatternView(this.templateID).subscribe(
      (view) => {
        this.expertView = view;
      }
    );
  }

}
