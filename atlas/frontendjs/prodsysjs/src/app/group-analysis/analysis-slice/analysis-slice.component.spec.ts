import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AnalysisSliceComponent } from './analysis-slice.component';

describe('AnalysisSliceComponent', () => {
  let component: AnalysisSliceComponent;
  let fixture: ComponentFixture<AnalysisSliceComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AnalysisSliceComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(AnalysisSliceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
