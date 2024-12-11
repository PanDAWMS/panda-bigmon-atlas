import { ComponentFixture, TestBed } from '@angular/core/testing';

import { JsondiffComponent } from './jsondiff.component';

describe('JsondiffComponent', () => {
  let component: JsondiffComponent;
  let fixture: ComponentFixture<JsondiffComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [JsondiffComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(JsondiffComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
