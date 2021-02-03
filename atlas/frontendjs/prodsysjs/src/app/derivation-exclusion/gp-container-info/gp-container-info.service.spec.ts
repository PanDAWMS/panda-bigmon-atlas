import { TestBed } from '@angular/core/testing';

import { GpContainerInfoService } from './gp-container-info.service';

describe('GpContainerInfoService', () => {
  let service: GpContainerInfoService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(GpContainerInfoService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
