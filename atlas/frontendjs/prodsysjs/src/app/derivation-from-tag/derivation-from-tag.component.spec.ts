import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DerivationFromTagComponent } from './derivation-from-tag.component';

describe('DerivationFromTagComponent', () => {
  let component: DerivationFromTagComponent;
  let fixture: ComponentFixture<DerivationFromTagComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DerivationFromTagComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DerivationFromTagComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
