import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ProductionRequestComponent } from './production-request.component';

describe('ProductionRequestComponent', () => {
  let component: ProductionRequestComponent;
  let fixture: ComponentFixture<ProductionRequestComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ProductionRequestComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ProductionRequestComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
