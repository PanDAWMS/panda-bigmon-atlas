import { TestBed } from '@angular/core/testing';

import { DerivationExtensionService } from './derivation-extension.service';

describe('DerivationExtensionService', () => {
  let service: DerivationExtensionService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(DerivationExtensionService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
