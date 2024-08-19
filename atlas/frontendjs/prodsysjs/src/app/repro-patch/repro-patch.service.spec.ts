import { TestBed } from '@angular/core/testing';

import { ReproPatchService } from './repro-patch.service';

describe('ReproPatchService', () => {
  let service: ReproPatchService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(ReproPatchService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
