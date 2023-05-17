import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DynamicFormWFilterComponent } from './dynamic-form-w-filter.component';

describe('DynamicFormWFilterComponent', () => {
  let component: DynamicFormWFilterComponent;
  let fixture: ComponentFixture<DynamicFormWFilterComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DynamicFormWFilterComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DynamicFormWFilterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
