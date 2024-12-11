import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ProdsysJsoneditorComponent } from './prodsys-jsoneditor.component';

describe('ProdsysJsoneditorComponent', () => {
  let component: ProdsysJsoneditorComponent;
  let fixture: ComponentFixture<ProdsysJsoneditorComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ProdsysJsoneditorComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ProdsysJsoneditorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
