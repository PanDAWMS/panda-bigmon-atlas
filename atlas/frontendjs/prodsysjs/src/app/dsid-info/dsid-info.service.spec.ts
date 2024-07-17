import { TestBed } from '@angular/core/testing';

import { DsidInfoService } from './dsid-info.service';

describe('DsidInfoService', () => {
  let service: DsidInfoService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(DsidInfoService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
