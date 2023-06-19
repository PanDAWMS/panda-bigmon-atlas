import { Component, OnInit } from '@angular/core';
import {DerivationDAODDerivation, DerivationPhysPatternService, PatternStep} from "./derivation-phys-pattern.service";
import {BehaviorSubject} from "rxjs";
import {switchMap, tap} from "rxjs/operators";
import {FormArray, FormBuilder, FormControl} from "@angular/forms";

@Component({
  selector: 'app-derivation-phys-pattern',
  templateUrl: './derivation-phys-pattern.component.html',
  styleUrls: ['./derivation-phys-pattern.component.css']
})
export class DerivationPhysPatternComponent implements OnInit {

  private updatePattern$ = new BehaviorSubject<boolean>(true);
  public currentPatterns: DerivationDAODDerivation[] = [];
  public patternSteps: (PatternStep[]|null)[] = [];
  public mcCampaigns: string[] = [];
  public PATTERN_STATUS = ['Active', 'Disabled'];
  public saveMessage = '';
  public patterns$ = this.updatePattern$.pipe(switchMap(() => this.derivationPhysPatternService.getPatternWithCampaigns()),
    tap((result) => {
      this.currentPatterns = result.current_patterns;
      for (const campaign of result.mc_campaigns){
        for (const subcampaign of campaign.subcampaigns){
          this.mcCampaigns.push(campaign.campaign + ':' + subcampaign);
        }
      }
      this.patternSteps = result.steps;
      this.mcPatternsForm = this.fb.group({mainArray: this.fb.array([])});
      for (const pattern in this.currentPatterns){
        const currentPatternGroup =  this.fb.group({
          campaign: [this.currentPatterns[pattern].campaign + ':' + this.currentPatterns[pattern].subcampaign, ''],
          train_id: [this.currentPatterns[pattern].request_id, ''],
          status: [this.currentPatterns[pattern].status, '']
        });
        this.mainArray.push(currentPatternGroup);

    }
    }));
  public mcPatternsForm =  this.fb.group({mainArray: this.fb.array([])});
  constructor(private derivationPhysPatternService: DerivationPhysPatternService,  private fb: FormBuilder) { }

  ngOnInit(): void {
  }

  onSubmit() {
    this.saveMessage = "Saving...";
    const newPatterns: DerivationDAODDerivation[] = [];
    const checkedCampaigns = [];
    let campaignDuplication = false;
    for (const pattern of this.mainArray.controls) {
      const patternValue = pattern.value;
      const patternCampaign = patternValue.campaign.split(':');
      if (checkedCampaigns.includes(patternValue.campaign) || checkedCampaigns.includes(patternCampaign[0]+':all')){
        this.saveMessage = `Error: Campaign ${patternValue.campaign} is duplicated`;
        campaignDuplication = true;
        break;
      }
      checkedCampaigns.push(patternValue.campaign);
      const newPattern: DerivationDAODDerivation = {
        campaign: patternCampaign[0],
        subcampaign: patternCampaign[1],
        outputs: ['DAOD_PHYS.DAOD_PHYSLITE'],
        request_id: patternValue.train_id,
        status: patternValue.status
      };
      newPatterns.push(newPattern);
    }
    if (!campaignDuplication) {
      this.derivationPhysPatternService.setPatternByRequestID(newPatterns).subscribe((result) => {
        this.saveMessage = "Saved";
        this.updatePattern$.next(true);
      }, (error) => {
        this.saveMessage = `Error ${error?.error}`;
      });
    }
  }
  get mainArray(): FormArray {
    return this.mcPatternsForm.get('mainArray') as FormArray;
  }

  addPattern() {
    this.mainArray.push(this.fb.group({
      campaign: ['', ''],
      train_id: ['', ''],
      status: ['Active', '']
    }));
  }
  requestIDChanged($event: Event, i: number) {
    this.patternSteps[i] = null;
  }
}
