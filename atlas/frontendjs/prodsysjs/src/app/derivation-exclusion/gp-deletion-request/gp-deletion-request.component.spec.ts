import { ComponentFixture, TestBed } from '@angular/core/testing';

import { GpDeletionRequestComponent } from './gp-deletion-request.component';

describe('GpDeletionRequestComponent', () => {
  let component: GpDeletionRequestComponent;
  let fixture: ComponentFixture<GpDeletionRequestComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ GpDeletionRequestComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(GpDeletionRequestComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
