import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PatternEditComponent } from './pattern-edit.component';

describe('PatternEditComponent', () => {
  let component: PatternEditComponent;
  let fixture: ComponentFixture<PatternEditComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ PatternEditComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(PatternEditComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
