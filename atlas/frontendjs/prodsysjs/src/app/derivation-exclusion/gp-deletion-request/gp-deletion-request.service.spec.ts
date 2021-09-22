import { TestBed } from '@angular/core/testing';

import { GpDeletionRequestService } from './gp-deletion-request.service';

describe('GpDeletionRequestService', () => {
  let service: GpDeletionRequestService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(GpDeletionRequestService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
