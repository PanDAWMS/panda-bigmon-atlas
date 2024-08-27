import { TestBed } from '@angular/core/testing';

import { TaskStageProfileService } from './task-stage-profile.service';

describe('TaskStageProfileService', () => {
  let service: TaskStageProfileService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(TaskStageProfileService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
