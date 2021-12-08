import { TestBed } from '@angular/core/testing';

import { UnmergeCleaningResolver } from './unmerge-cleaning.resolver';

describe('UnmergeCleaningResolver', () => {
  let resolver: UnmergeCleaningResolver;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    resolver = TestBed.inject(UnmergeCleaningResolver);
  });

  it('should be created', () => {
    expect(resolver).toBeTruthy();
  });
});
