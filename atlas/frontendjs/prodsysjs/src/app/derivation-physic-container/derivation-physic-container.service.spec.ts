import { TestBed } from '@angular/core/testing';

import { DerivationPhysicContainerService } from './derivation-physic-container.service';

describe('DerivationPhysicContainerService', () => {
  let service: DerivationPhysicContainerService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(DerivationPhysicContainerService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
