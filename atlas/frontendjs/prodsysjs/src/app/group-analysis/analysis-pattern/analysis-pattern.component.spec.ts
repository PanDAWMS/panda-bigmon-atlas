import { TestBed } from '@angular/core/testing';
import { FormBuilder } from '@angular/forms';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { of, Subject } from 'rxjs';
import { AnalysisTasksService } from '../analysis-tasks.service';
import { TemplateBase } from '../analysis-task-model';
import { AnalysisPatternComponent } from './analysis-pattern.component';

describe('AnalysisPatternComponent Source Tar update', () => {
  let component: AnalysisPatternComponent;
  let service: jasmine.SpyObj<AnalysisTasksService>;
  let update: Subject<TemplateBase>;

  beforeEach(() => {
    service = jasmine.createSpyObj('AnalysisTasksService', ['getTemplate', 'updateSourceTar']);
    service.getTemplate.and.returnValue(of({status: 'ACTIVE'} as TemplateBase));
    update = new Subject<TemplateBase>();
    service.updateSourceTar.and.returnValue(update);
    TestBed.configureTestingModule({
      providers: [
        FormBuilder,
        {provide: AnalysisTasksService, useValue: service},
        {provide: ActivatedRoute, useValue: {paramMap: of(convertToParamMap({tag: 'a123'}))}}
      ]
    });
    component = TestBed.runInInjectionContext(() => new AnalysisPatternComponent());
  });

  it('updates the current tag and reloads its template on success', () => {
    const subscription = component.taskTemplate$.subscribe();
    component.sourceTaskID.setValue(12345);
    component.updateSourceTar();
    expect(service.updateSourceTar).toHaveBeenCalledWith('a123', 12345);
    expect(component.updatingSourceTar).toBeTrue();
    component.updateSourceTar();
    expect(service.updateSourceTar).toHaveBeenCalledTimes(1);
    update.next({} as TemplateBase);
    update.complete();
    expect(service.getTemplate).toHaveBeenCalledTimes(2);
    expect(component.sourceTaskID.value).toBeNull();
    expect(component.sourceTarMessage).toContain('Upload to Rucio is queued');
    expect(component.updatingSourceTar).toBeFalse();
    subscription.unsubscribe();
  });

  it('rejects empty, non-positive, fractional and unsafe task IDs', () => {
    for (const taskID of [null, 0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      component.sourceTaskID.setValue(taskID);
      component.updateSourceTar();
    }
    expect(service.updateSourceTar).not.toHaveBeenCalled();
  });

  it('shows backend errors and keeps the task ID for retry', () => {
    component.sourceTaskID.setValue(12345);
    component.updateSourceTar();
    update.error({error: 'Source tar does not exist for task 12345'});
    expect(component.errorMessage).toBe('Source tar does not exist for task 12345');
    expect(component.sourceTaskID.value).toBe(12345);
    expect(component.updatingSourceTar).toBeFalse();
    expect(service.getTemplate).not.toHaveBeenCalled();
  });
});
