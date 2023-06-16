import { TestBed } from '@angular/core/testing';

import { DerivationPhysPatternService } from './derivation-phys-pattern.service';

describe('DerivationPhysPatternService', () => {
  let service: DerivationPhysPatternService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(DerivationPhysPatternService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
