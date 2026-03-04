import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EventPickingRequestComponent } from './event-picking-request.component';

describe('EventPickingRequestComponent', () => {
  let component: EventPickingRequestComponent;
  let fixture: ComponentFixture<EventPickingRequestComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [EventPickingRequestComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(EventPickingRequestComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
