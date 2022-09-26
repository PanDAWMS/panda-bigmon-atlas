import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TaskActionComponent } from './task-action.component';

describe('TaskActionComponent', () => {
  let component: TaskActionComponent;
  let fixture: ComponentFixture<TaskActionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TaskActionComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(TaskActionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
