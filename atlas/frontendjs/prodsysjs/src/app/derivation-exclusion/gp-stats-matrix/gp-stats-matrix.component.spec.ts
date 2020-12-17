import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GpStatsMatrixComponent } from './gp-stats-matrix.component';

describe('GpStatsMatrixComponent', () => {
  let component: GpStatsMatrixComponent;
  let fixture: ComponentFixture<GpStatsMatrixComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GpStatsMatrixComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GpStatsMatrixComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
