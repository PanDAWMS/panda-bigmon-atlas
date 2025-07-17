import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RucioContainerComponent } from './rucio-container.component';

describe('RucioContainerComponent', () => {
  let component: RucioContainerComponent;
  let fixture: ComponentFixture<RucioContainerComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RucioContainerComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(RucioContainerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
