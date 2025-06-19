import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DatatsetDetailsComponent } from './datatset-details.component';

describe('DatatsetDetailsComponent', () => {
  let component: DatatsetDetailsComponent;
  let fixture: ComponentFixture<DatatsetDetailsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DatatsetDetailsComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DatatsetDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
