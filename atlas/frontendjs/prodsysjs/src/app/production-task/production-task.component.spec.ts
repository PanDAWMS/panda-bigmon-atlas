import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ProductionTaskComponent } from './production-task.component';

describe('ProductionTaskComponent', () => {
  let component: ProductionTaskComponent;
  let fixture: ComponentFixture<ProductionTaskComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ProductionTaskComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ProductionTaskComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
