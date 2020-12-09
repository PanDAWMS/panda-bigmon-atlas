import { TestBed } from '@angular/core/testing';

import { GPDeletionContainerService } from './gp-deleation.service';

describe('GpDeleationService', () => {
  let service: GPDeletionContainerService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(GPDeletionContainerService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
