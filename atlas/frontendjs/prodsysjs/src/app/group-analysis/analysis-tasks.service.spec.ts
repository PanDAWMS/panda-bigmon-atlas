import { TestBed } from '@angular/core/testing';

import { AnalysisTasksService } from './analysis-tasks.service';

describe('AnalysisTasksService', () => {
  let service: AnalysisTasksService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(AnalysisTasksService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
