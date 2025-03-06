import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PatternSelectionComponent } from './pattern-selection.component';

describe('PatternSelectionComponent', () => {
  let component: PatternSelectionComponent;
  let fixture: ComponentFixture<PatternSelectionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PatternSelectionComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(PatternSelectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
