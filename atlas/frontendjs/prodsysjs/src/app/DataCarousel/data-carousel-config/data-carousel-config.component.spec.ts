import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DataCarouselConfigComponent } from './data-carousel-config.component';

describe('DataCarouselConfigComponent', () => {
  let component: DataCarouselConfigComponent;
  let fixture: ComponentFixture<DataCarouselConfigComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DataCarouselConfigComponent]
    });
    fixture = TestBed.createComponent(DataCarouselConfigComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
