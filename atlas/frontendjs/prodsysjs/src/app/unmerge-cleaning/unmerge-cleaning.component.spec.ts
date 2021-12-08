import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UnmergeCleaningComponent } from './unmerge-cleaning.component';

describe('UnmergeCleaningComponent', () => {
  let component: UnmergeCleaningComponent;
  let fixture: ComponentFixture<UnmergeCleaningComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ UnmergeCleaningComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UnmergeCleaningComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
