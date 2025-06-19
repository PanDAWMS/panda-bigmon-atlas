import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RucioDIDComponent } from './rucio-did.component';

describe('RucioDIDComponent', () => {
  let component: RucioDIDComponent;
  let fixture: ComponentFixture<RucioDIDComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RucioDIDComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(RucioDIDComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
