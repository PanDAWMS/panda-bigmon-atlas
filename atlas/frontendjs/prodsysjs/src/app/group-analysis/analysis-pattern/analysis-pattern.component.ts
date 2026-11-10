import { Component, OnInit, inject } from '@angular/core';
import {ActivatedRoute} from "@angular/router";
import {AnalysisTasksService} from "../analysis-tasks.service";
import {catchError, finalize, startWith, switchMap, tap} from "rxjs/operators";
import {of, Subject} from "rxjs";
import {editState, PatternChanges} from "../pattern-edit/pattern-edit.component";
import {FormBuilder, FormControl, Validators} from "@angular/forms";

@Component({
    selector: 'app-analysis-pattern',
    templateUrl: './analysis-pattern.component.html',
    styleUrls: ['./analysis-pattern.component.css'],
    standalone: false
})
export class AnalysisPatternComponent implements OnInit {
  private route = inject(ActivatedRoute);
  private analysisTaskService = inject(AnalysisTasksService);
  private fb = inject(FormBuilder);


  public templateID: string;
  public errorMessage = '';
  private refreshTemplate$ = new Subject<void>();
  public taskTemplate$  = this.route.paramMap.pipe(switchMap((params) => {
    this.templateID = params.get('tag');
    return this.refreshTemplate$.pipe(startWith(undefined),
      switchMap(() => this.analysisTaskService.getTemplate(params.get('tag'))));
  }), tap((taskTemplate) => {this.templateForm.patchValue({status: taskTemplate.status}); }));
  public templateForm = this.fb.group({status: ['']});
  public editMode: editState = 'view';
  public loading = false;
  public expertView = 'Loading...';
  public sourceTaskID = new FormControl<number | null>(null,
    [Validators.required, Validators.min(1), Validators.pattern(/^[0-9]+$/)]);
  public updatingSourceTar = false;
  public sourceTarMessage = '';

  updateSourceTar(): void {
    if (this.sourceTaskID.invalid || !Number.isSafeInteger(this.sourceTaskID.value) || this.updatingSourceTar) {
      return;
    }
    this.errorMessage = '';
    this.sourceTarMessage = '';
    this.updatingSourceTar = true;
    this.analysisTaskService.updateSourceTar(this.templateID, this.sourceTaskID.value).pipe(
      finalize(() => { this.updatingSourceTar = false; })
    ).subscribe({
      next: () => {
        this.sourceTaskID.reset();
        this.sourceTarMessage = 'Source Tar updated. Upload to Rucio is queued; reload the page to see the archive link once it completes.';
        this.refreshTemplate$.next();
      },
      error: (err) => {
        this.errorMessage = typeof err.error === 'string' ? err.error : JSON.stringify(err.error) || err.message;
      }
    });
  }

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
