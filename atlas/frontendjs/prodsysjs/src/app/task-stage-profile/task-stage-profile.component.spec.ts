import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TaskStageProfileComponent } from './task-stage-profile.component';

describe('TaskStageProfileComponent', () => {
  let component: TaskStageProfileComponent;
  let fixture: ComponentFixture<TaskStageProfileComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TaskStageProfileComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(TaskStageProfileComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
