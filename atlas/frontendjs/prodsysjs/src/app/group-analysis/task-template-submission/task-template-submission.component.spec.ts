import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TaskTemplateSubmissionComponent } from './task-template-submission.component';

describe('TaskTemplateSubmissionComponent', () => {
  let component: TaskTemplateSubmissionComponent;
  let fixture: ComponentFixture<TaskTemplateSubmissionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TaskTemplateSubmissionComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(TaskTemplateSubmissionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
