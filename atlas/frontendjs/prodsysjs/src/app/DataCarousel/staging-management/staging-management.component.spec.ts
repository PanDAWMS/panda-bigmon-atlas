import { ComponentFixture, TestBed } from '@angular/core/testing';

import { StagingManagementComponent } from './staging-management.component';

describe('StagingManagementComponent', () => {
  let component: StagingManagementComponent;
  let fixture: ComponentFixture<StagingManagementComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [StagingManagementComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(StagingManagementComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
