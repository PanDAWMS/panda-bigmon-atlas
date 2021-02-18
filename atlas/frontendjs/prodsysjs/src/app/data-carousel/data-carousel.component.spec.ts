import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing';

import { DataCarouselComponent } from './data-carousel.component';

describe('DataCarouselComponent', () => {
  let component: DataCarouselComponent;
  let fixture: ComponentFixture<DataCarouselComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [ DataCarouselComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DataCarouselComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
