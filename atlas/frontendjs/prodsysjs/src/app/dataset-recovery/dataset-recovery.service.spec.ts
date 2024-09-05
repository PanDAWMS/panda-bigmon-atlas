import { TestBed } from '@angular/core/testing';

import { DatasetRecoveryService } from './dataset-recovery.service';

describe('DatasetRecoveryService', () => {
  let service: DatasetRecoveryService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(DatasetRecoveryService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
