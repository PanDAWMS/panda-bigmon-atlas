import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing';

import { DatasetsTableComponent } from './datasets-table.component';

describe('DatasetsTableComponent', () => {
  let component: DatasetsTableComponent;
  let fixture: ComponentFixture<DatasetsTableComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [ DatasetsTableComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DatasetsTableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
