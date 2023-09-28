import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ParentDrivationForAnalysisComponent } from './parent-drivation-for-analysis.component';

describe('ParentDrivationForAnalysisComponent', () => {
  let component: ParentDrivationForAnalysisComponent;
  let fixture: ComponentFixture<ParentDrivationForAnalysisComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [ParentDrivationForAnalysisComponent]
    });
    fixture = TestBed.createComponent(ParentDrivationForAnalysisComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
