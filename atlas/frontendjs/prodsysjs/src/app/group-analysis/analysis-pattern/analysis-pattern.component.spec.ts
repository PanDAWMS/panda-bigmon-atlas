import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AnalysisPatternComponent } from './analysis-pattern.component';

describe('AnalysisPatternComponent', () => {
  let component: AnalysisPatternComponent;
  let fixture: ComponentFixture<AnalysisPatternComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AnalysisPatternComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(AnalysisPatternComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
