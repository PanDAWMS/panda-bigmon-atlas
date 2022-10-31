import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TaskStagingProgressComponent } from './task-staging-progress.component';

describe('TaskStagingProgressComponent', () => {
  let component: TaskStagingProgressComponent;
  let fixture: ComponentFixture<TaskStagingProgressComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TaskStagingProgressComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(TaskStagingProgressComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
