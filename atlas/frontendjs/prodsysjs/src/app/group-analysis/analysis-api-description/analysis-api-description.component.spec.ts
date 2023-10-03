import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AnalysisApiDescriptionComponent } from './analysis-api-description.component';

describe('AnalysisApiDescriptionComponent', () => {
  let component: AnalysisApiDescriptionComponent;
  let fixture: ComponentFixture<AnalysisApiDescriptionComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [AnalysisApiDescriptionComponent]
    });
    fixture = TestBed.createComponent(AnalysisApiDescriptionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
