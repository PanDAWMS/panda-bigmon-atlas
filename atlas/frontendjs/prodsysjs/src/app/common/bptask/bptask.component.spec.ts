import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BPTaskComponent } from './bptask.component';

describe('BPTaskComponent', () => {
  let component: BPTaskComponent;
  let fixture: ComponentFixture<BPTaskComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ BPTaskComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(BPTaskComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
