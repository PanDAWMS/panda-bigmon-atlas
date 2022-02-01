import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ChipsMultiSelectColoredComponent } from './multi-select-colored-chips.component';

describe('MultiSelectColoredChipsComponent', () => {
  let component: ChipsMultiSelectColoredComponent;
  let fixture: ComponentFixture<ChipsMultiSelectColoredComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ChipsMultiSelectColoredComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ChipsMultiSelectColoredComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
