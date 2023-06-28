import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateAnalysisRequestComponent } from './create-analysis-request.component';

describe('CreateAnalysisRequestComponent', () => {
  let component: CreateAnalysisRequestComponent;
  let fixture: ComponentFixture<CreateAnalysisRequestComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CreateAnalysisRequestComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CreateAnalysisRequestComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
