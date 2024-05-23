import { ComponentFixture, TestBed } from '@angular/core/testing';

import { McRequestSubmissionComponent } from './mc-request-submission.component';

describe('McRequestSubmissionComponent', () => {
  let component: McRequestSubmissionComponent;
  let fixture: ComponentFixture<McRequestSubmissionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [McRequestSubmissionComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(McRequestSubmissionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
