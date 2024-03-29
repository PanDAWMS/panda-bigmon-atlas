import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing';

import { GpStatsComponent } from './gp-stats.component';

describe('GpStatsComponent', () => {
  let component: GpStatsComponent;
  let fixture: ComponentFixture<GpStatsComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [ GpStatsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GpStatsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
