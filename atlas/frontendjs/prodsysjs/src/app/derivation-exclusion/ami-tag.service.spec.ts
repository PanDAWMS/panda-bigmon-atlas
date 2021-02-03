import { TestBed } from '@angular/core/testing';

import { AmiTagService } from './ami-tag.service';

describe('AmiTagService', () => {
  let service: AmiTagService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(AmiTagService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
