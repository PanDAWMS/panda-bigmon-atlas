import { TestBed } from '@angular/core/testing';

import { ProductionRequestService } from './production-request.service';

describe('ProductionRequestService', () => {
  let service: ProductionRequestService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(ProductionRequestService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
