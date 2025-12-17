import { TestBed } from '@angular/core/testing';

import { LostFileRecoveryService } from './lost-file-recovery.service';

describe('LostFileRecoveryService', () => {
  let service: LostFileRecoveryService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(LostFileRecoveryService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
