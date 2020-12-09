import { TestBed } from '@angular/core/testing';

import { GpContainerDetailsService } from './gp-container-details.service';

describe('GpContainerDetailsService', () => {
  let service: GpContainerDetailsService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(GpContainerDetailsService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
