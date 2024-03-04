import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TaskManagementByUrlComponent } from './task-management-by-url.component';

describe('TaskManagementByUrlComponent', () => {
  let component: TaskManagementByUrlComponent;
  let fixture: ComponentFixture<TaskManagementByUrlComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TaskManagementByUrlComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(TaskManagementByUrlComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
