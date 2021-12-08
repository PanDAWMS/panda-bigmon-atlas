import { TestBed } from '@angular/core/testing';

import { UnmergeCleaningService } from './unmerge-cleaning.service';

describe('UnmergeCleaningService', () => {
  let service: UnmergeCleaningService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(UnmergeCleaningService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
