import { TestBed } from '@angular/core/testing';

import { EventPickingService } from './event-picking.service';

describe('EventPickingService', () => {
  let service: EventPickingService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(EventPickingService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
