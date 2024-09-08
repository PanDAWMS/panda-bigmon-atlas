import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RecoveryRequestsApproveComponent } from './recovery-requests-approve.component';

describe('RecoveryRequestsApproveComponent', () => {
  let component: RecoveryRequestsApproveComponent;
  let fixture: ComponentFixture<RecoveryRequestsApproveComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RecoveryRequestsApproveComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(RecoveryRequestsApproveComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
