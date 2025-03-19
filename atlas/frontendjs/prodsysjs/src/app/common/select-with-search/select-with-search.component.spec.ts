import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SelectWithSearchComponent } from './select-with-search.component';

describe('SelectWithSearchComponent', () => {
  let component: SelectWithSearchComponent;
  let fixture: ComponentFixture<SelectWithSearchComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SelectWithSearchComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(SelectWithSearchComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
