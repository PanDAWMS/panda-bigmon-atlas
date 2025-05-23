import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AsyncTaskProgressComponent } from './async-task-progress.component';

describe('AsyncTaskProgressComponent', () => {
  let component: AsyncTaskProgressComponent;
  let fixture: ComponentFixture<AsyncTaskProgressComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [AsyncTaskProgressComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(AsyncTaskProgressComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
