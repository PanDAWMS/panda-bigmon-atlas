import { TestBed } from '@angular/core/testing';

import { GpStatsService } from './gp-stats.service';

describe('GpStatsService', () => {
  let service: GpStatsService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(GpStatsService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
