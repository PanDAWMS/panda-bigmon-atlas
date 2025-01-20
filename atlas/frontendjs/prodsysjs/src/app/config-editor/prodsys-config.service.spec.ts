import { TestBed } from '@angular/core/testing';

import { ProdsysConfigService } from './prodsys-config.service';

describe('ProdsysConfigService', () => {
  let service: ProdsysConfigService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(ProdsysConfigService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
