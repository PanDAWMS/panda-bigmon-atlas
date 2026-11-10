import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

import { AnalysisTasksService } from './analysis-tasks.service';

describe('AnalysisTasksService', () => {
  let service: AnalysisTasksService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({imports: [HttpClientTestingModule]});
    service = TestBed.inject(AnalysisTasksService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  it('loads the allowed container scopes', () => {
    service.getAnalysisContainerScopes().subscribe(scopes => expect(scopes).toEqual(['user.analyst']));
    http.expectOne('/api/get_analysis_container_scopes/').flush(['user.analyst']);
  });

  it('posts the selected datasets and unprefixed name to create a container', () => {
    service.registerAnalysisOutputContainer('1234', 'user.analyst', 'results', ['dataset']).subscribe();
    const request = http.expectOne('/api/register_analysis_output_container/');
    expect(request.request.method).toBe('POST');
    expect(request.request.body).toEqual({
      requestID: '1234', scope: 'user.analyst', name: 'results', datasets: ['dataset']
    });
    request.flush('Container created');
  });

  it('posts the tag and source task ID to update Source Tar', () => {
    service.updateSourceTar('a123', 12345).subscribe();
    const request = http.expectOne('/api/update_source_tar/');
    expect(request.request.method).toBe('POST');
    expect(request.request.body).toEqual({tag: 'a123', task_id: 12345});
    request.flush({tag: 'a123', source_tar: ''});
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
