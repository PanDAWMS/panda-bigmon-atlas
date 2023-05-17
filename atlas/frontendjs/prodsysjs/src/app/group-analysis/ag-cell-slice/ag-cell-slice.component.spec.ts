import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AgCellSliceComponent } from './ag-cell-slice.component';

describe('AgCellSliceComponent', () => {
  let component: AgCellSliceComponent;
  let fixture: ComponentFixture<AgCellSliceComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AgCellSliceComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(AgCellSliceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
