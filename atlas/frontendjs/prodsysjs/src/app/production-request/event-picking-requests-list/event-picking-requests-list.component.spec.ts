import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EventPickingRequestsListComponent } from './event-picking-requests-list.component';

describe('EventPickingRequestsListComponent', () => {
  let component: EventPickingRequestsListComponent;
  let fixture: ComponentFixture<EventPickingRequestsListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [EventPickingRequestsListComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(EventPickingRequestsListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
