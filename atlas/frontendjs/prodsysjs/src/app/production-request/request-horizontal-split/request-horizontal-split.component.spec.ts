import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RequestHorizontalSplitComponent } from './request-horizontal-split.component';

describe('RequestHorizontalSplitComponent', () => {
  let component: RequestHorizontalSplitComponent;
  let fixture: ComponentFixture<RequestHorizontalSplitComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RequestHorizontalSplitComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(RequestHorizontalSplitComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
