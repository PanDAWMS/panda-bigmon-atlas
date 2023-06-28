import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AnalyRequestActionsComponent } from './analy-request-actions.component';

describe('AnalyRequestActionsComponent', () => {
  let component: AnalyRequestActionsComponent;
  let fixture: ComponentFixture<AnalyRequestActionsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AnalyRequestActionsComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(AnalyRequestActionsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
