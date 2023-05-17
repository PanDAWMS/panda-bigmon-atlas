import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AnalysisRequestComponent } from './analysis-request.component';

describe('AnalysisRequestComponent', () => {
  let component: AnalysisRequestComponent;
  let fixture: ComponentFixture<AnalysisRequestComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AnalysisRequestComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(AnalysisRequestComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
