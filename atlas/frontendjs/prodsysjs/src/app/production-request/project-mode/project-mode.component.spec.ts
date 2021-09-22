import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ProjectModeComponent } from './project-mode.component';

describe('ProjectModeComponent', () => {
  let component: ProjectModeComponent;
  let fixture: ComponentFixture<ProjectModeComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ProjectModeComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ProjectModeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
