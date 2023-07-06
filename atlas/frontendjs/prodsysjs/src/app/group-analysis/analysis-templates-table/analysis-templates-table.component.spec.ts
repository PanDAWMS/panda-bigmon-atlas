import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AnalysisTemplatesTableComponent } from './analysis-templates-table.component';

describe('AnalysisTemplatesTableComponent', () => {
  let component: AnalysisTemplatesTableComponent;
  let fixture: ComponentFixture<AnalysisTemplatesTableComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AnalysisTemplatesTableComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(AnalysisTemplatesTableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
