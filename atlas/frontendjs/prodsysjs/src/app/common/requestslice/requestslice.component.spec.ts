import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RequestsliceComponent } from './requestslice.component';

describe('RequestsliceComponent', () => {
  let component: RequestsliceComponent;
  let fixture: ComponentFixture<RequestsliceComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ RequestsliceComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(RequestsliceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
