import { TestBed } from '@angular/core/testing';

import { JsonGdpconfigEditorService } from './json-gdpconfig-editor.service';

describe('JsonGdpconfigEditorService', () => {
  let service: JsonGdpconfigEditorService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(JsonGdpconfigEditorService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
