import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ProductionTaskTableComponent } from './production-task-table.component';

describe('ProductionTaskTableComponent', () => {
  let component: ProductionTaskTableComponent;
  let fixture: ComponentFixture<ProductionTaskTableComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ProductionTaskTableComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ProductionTaskTableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
