import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DerivationPhysicContainerComponent } from './derivation-physic-container.component';

describe('DerivationPhysicContainerComponent', () => {
  let component: DerivationPhysicContainerComponent;
  let fixture: ComponentFixture<DerivationPhysicContainerComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DerivationPhysicContainerComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(DerivationPhysicContainerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
