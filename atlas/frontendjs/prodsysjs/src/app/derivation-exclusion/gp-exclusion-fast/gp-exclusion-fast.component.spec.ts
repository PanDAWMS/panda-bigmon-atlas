import { ComponentFixture, TestBed } from '@angular/core/testing';

import { GpExclusionFastComponent } from './gp-exclusion-fast.component';

describe('GpExclusionFastComponent', () => {
  let component: GpExclusionFastComponent;
  let fixture: ComponentFixture<GpExclusionFastComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [GpExclusionFastComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(GpExclusionFastComponent);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
