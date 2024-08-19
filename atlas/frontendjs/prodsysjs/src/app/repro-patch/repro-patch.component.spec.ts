import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ReproPatchComponent } from './repro-patch.component';

describe('ReproPatchComponent', () => {
  let component: ReproPatchComponent;
  let fixture: ComponentFixture<ReproPatchComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ReproPatchComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ReproPatchComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
