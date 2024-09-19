import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RecoveredDatasetsComponent } from './recovered-datasets.component';

describe('RecoveredDatasetsComponent', () => {
  let component: RecoveredDatasetsComponent;
  let fixture: ComponentFixture<RecoveredDatasetsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RecoveredDatasetsComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(RecoveredDatasetsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
