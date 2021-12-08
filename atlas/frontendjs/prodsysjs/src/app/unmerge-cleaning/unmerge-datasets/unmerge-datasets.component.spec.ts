import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UnmergeDatasetsComponent } from './unmerge-datasets.component';

describe('UnmergeDatasetsComponent', () => {
  let component: UnmergeDatasetsComponent;
  let fixture: ComponentFixture<UnmergeDatasetsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ UnmergeDatasetsComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UnmergeDatasetsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
