import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DsidInfoComponent } from './dsid-info.component';

describe('DsidInfoComponent', () => {
  let component: DsidInfoComponent;
  let fixture: ComponentFixture<DsidInfoComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DsidInfoComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DsidInfoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
