import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing';

import { DerivationExclusionComponent } from './derivation-exclusion.component';

describe('DerivationExclusionComponent', () => {
  let component: DerivationExclusionComponent;
  let fixture: ComponentFixture<DerivationExclusionComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [ DerivationExclusionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DerivationExclusionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
