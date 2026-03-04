import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EventPickingRequestCreationComponent } from './event-picking-request-creation.component';

describe('EventPickingRequestCreationComponent', () => {
  let component: EventPickingRequestCreationComponent;
  let fixture: ComponentFixture<EventPickingRequestCreationComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [EventPickingRequestCreationComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(EventPickingRequestCreationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
