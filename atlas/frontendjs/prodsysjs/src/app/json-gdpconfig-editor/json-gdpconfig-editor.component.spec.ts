import { ComponentFixture, TestBed } from '@angular/core/testing';

import { JsonGDPConfigEditorComponent } from './json-gdpconfig-editor.component';

describe('JsonGDPConfigEditorComponent', () => {
  let component: JsonGDPConfigEditorComponent;
  let fixture: ComponentFixture<JsonGDPConfigEditorComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [JsonGDPConfigEditorComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(JsonGDPConfigEditorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
