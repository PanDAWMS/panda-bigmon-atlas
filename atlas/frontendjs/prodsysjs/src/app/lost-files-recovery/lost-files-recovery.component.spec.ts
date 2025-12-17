import { ComponentFixture, TestBed } from '@angular/core/testing';

import { LostFilesRecoveryComponent } from './lost-files-recovery.component';

describe('LostFilesRecoveryComponent', () => {
  let component: LostFilesRecoveryComponent;
  let fixture: ComponentFixture<LostFilesRecoveryComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [LostFilesRecoveryComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(LostFilesRecoveryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
