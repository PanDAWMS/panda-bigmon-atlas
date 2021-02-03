import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GpApiInstructionComponent } from './gp-api-instruction.component';

describe('GpApiInstructionComponent', () => {
  let component: GpApiInstructionComponent;
  let fixture: ComponentFixture<GpApiInstructionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GpApiInstructionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GpApiInstructionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
