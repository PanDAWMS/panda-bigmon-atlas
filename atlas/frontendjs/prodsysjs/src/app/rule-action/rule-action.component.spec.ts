import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RuleActionComponent } from './rule-action.component';

describe('RuleActionComponent', () => {
  let component: RuleActionComponent;
  let fixture: ComponentFixture<RuleActionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RuleActionComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(RuleActionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
