import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PMGApprovalComponent } from './pmgapproval.component';

describe('PMGApprovalComponent', () => {
  let component: PMGApprovalComponent;
  let fixture: ComponentFixture<PMGApprovalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PMGApprovalComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(PMGApprovalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
