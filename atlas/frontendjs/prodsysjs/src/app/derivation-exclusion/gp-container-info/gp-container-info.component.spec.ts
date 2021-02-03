import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GpContainerInfoComponent } from './gp-container-info.component';

describe('GpContainerInfoComponent', () => {
  let component: GpContainerInfoComponent;
  let fixture: ComponentFixture<GpContainerInfoComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GpContainerInfoComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GpContainerInfoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
