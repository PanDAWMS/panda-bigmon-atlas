import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DerivationPhysPatternComponent } from './derivation-phys-pattern.component';

describe('DerivationPhysPatternComponent', () => {
  let component: DerivationPhysPatternComponent;
  let fixture: ComponentFixture<DerivationPhysPatternComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DerivationPhysPatternComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DerivationPhysPatternComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
