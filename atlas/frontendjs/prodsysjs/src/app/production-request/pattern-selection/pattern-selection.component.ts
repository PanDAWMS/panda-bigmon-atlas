import {Component, computed, input, OnInit, signal} from '@angular/core';
import {CampaignPattern, MCPattern, PatternSteps} from "../production-request-models";
import {toObservable} from "@angular/core/rxjs-interop";
import {NgClass} from "@angular/common";

interface CampaignPatternWithStyles {
  style: string;
  campaignPatterns: CampaignPattern[];
}

@Component({
  selector: 'app-pattern-selection',
  imports: [
    NgClass
  ],
  templateUrl: './pattern-selection.component.html',
  styleUrl: './pattern-selection.component.css'
})
export class PatternSelectionComponent {

  patterns = input<CampaignPattern[]>([]);

  campaignColors = [
    "bg-red-200",
    "bg-green-200",
    "bg-blue-200",
    "bg-yellow-200"
  ];
  stepArray = computed<PatternSteps[]>( () => {
    return this.patterns().reduce((acc, campaign) => {
      return acc.concat(campaign.patterns.reduce((acc2, pattern) => {
        return acc2.concat(pattern.steps);
      }, []));
    }, []);
  });
  patternArray = computed<MCPattern[]>( () => {
    return this.patterns().reduce((acc, campaign) => {
      return acc.concat(campaign.patterns);
    }, []);
  });

    getGridTemplate(): string {
      const totalColumns = this.stepArray().length;
      return `repeat(${totalColumns}, 1fr)`;
  }

    getColorClass(index: number): string {
    return this.campaignColors[index % this.campaignColors.length];
  }
}

