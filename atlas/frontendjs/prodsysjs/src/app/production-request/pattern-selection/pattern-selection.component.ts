import {Component, computed, input, OnInit, signal} from '@angular/core';
import {CampaignPattern, MCPattern} from "../production-request-models";
import {toObservable} from "@angular/core/rxjs-interop";

interface CampaignPatternWithStyles {
  style: string;
  campaignPatterns: CampaignPattern[];
}

@Component({
  selector: 'app-pattern-selection',
  imports: [],
  templateUrl: './pattern-selection.component.html',
  styleUrl: './pattern-selection.component.css'
})
export class PatternSelectionComponent implements OnInit {

  patterns = input(undefined, {transform: this.prepareStyles});

  campaignColors = [
    "bg-red-200",
    "bg-green-200",
    "bg-blue-200",
    "bg-yellow-200"
  ];

  styleReady = signal(false);

  campaignsWithStyles = computed(() => this.patterns().campaignPatterns.map((campaign, index) => {
    return {
      ...campaign,
       style: `col-span-${stepsInPattern(campaign.patterns)} font-bold text-gray-700 border-4 border-indigo-600 ${this.campaignColors[index % this.campaignColors.length]}`

    };
  }
  ));
inputObs$ = toObservable(this.patterns);
ngOnInit(): void {
  this.inputObs$.subscribe((campaigns) => {
    if (campaigns.campaignPatterns.length > 0) {
      this.styleReady.set(true);
    }
  });
}

  prepareStyles(patterns: CampaignPattern[]| undefined): CampaignPatternWithStyles {
    const totalSteps = patterns.reduce((acc, campaign) => acc +
      stepsInPattern(campaign.patterns), 0);
    return {
      campaignPatterns: patterns,
       style: `border-solid grid grid-cols-${totalSteps} cursor-pointer`
    };
  }
}
function  stepsInPattern(patterns: MCPattern[]): number {
    return patterns.reduce((acc2, pattern) => acc2 + pattern.steps.length, 0);
  }
