import { TestBed } from '@angular/core/testing';

import { DataCarouselService } from './data-carousel.service';

describe('DataCarouselService', () => {
  let service: DataCarouselService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(DataCarouselService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
