import { ComponentFixture, TestBed } from '@angular/core/testing';

import { McSubcampaignsStatComponent } from './mc-subcampaigns-stat.component';

describe('McSubcampaignsStatComponent', () => {
  let component: McSubcampaignsStatComponent;
  let fixture: ComponentFixture<McSubcampaignsStatComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [McSubcampaignsStatComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(McSubcampaignsStatComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
