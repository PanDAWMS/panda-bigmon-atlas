import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DerivationExtensionComponent } from './derivation-extension.component';

describe('DerivationExtensionComponent', () => {
  let component: DerivationExtensionComponent;
  let fixture: ComponentFixture<DerivationExtensionComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DerivationExtensionComponent]
    });
    fixture = TestBed.createComponent(DerivationExtensionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
