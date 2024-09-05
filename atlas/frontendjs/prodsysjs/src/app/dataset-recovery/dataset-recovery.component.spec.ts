import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DatasetRecoveryComponent } from './dataset-recovery.component';

describe('DatasetRecoveryComponent', () => {
  let component: DatasetRecoveryComponent;
  let fixture: ComponentFixture<DatasetRecoveryComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DatasetRecoveryComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DatasetRecoveryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
