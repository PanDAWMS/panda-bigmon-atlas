import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MultiRequestInitialComponent } from './multi-request-initial.component';

describe('MultiRequestInitialComponent', () => {
  let component: MultiRequestInitialComponent;
  let fixture: ComponentFixture<MultiRequestInitialComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [MultiRequestInitialComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(MultiRequestInitialComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
