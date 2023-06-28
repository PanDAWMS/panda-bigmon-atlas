import { ComponentFixture, TestBed } from '@angular/core/testing';

import { StepTasksComponent } from './step-tasks.component';

describe('StepTasksComponent', () => {
  let component: StepTasksComponent;
  let fixture: ComponentFixture<StepTasksComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ StepTasksComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(StepTasksComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
