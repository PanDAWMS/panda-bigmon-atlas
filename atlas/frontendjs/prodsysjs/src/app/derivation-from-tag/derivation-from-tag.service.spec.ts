import { TestBed } from '@angular/core/testing';

import { DerivationFromTagService } from './derivation-from-tag.service';

describe('DerivationFromTagService', () => {
  let service: DerivationFromTagService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(DerivationFromTagService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
