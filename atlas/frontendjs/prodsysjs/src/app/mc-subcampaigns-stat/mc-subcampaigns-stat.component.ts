import { Component } from '@angular/core';
import {MCSubCampaignStats, TaskService} from "../production-task/task-service.service";
import {AsyncPipe, JsonPipe, NgIf, NgTemplateOutlet} from "@angular/common";
import {catchError, map} from "rxjs/operators";
import {Observable} from "rxjs";
import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";
import {MatExpansionModule} from "@angular/material/expansion";
import {BillionPipe} from "./billion.pipe";


interface MCSubCampaignStatsInterface {
  mc_subcampaign: string;
  stats: {
    evgen: {
      total_events: number;
      tags: {
        tag: string;
        nevents: number;
      }[];
    };
    simul: {
      total_events: number;
      fastsim_events: number;
      fullsim_events: number;
      tags: {
        tag: string;
        nevents: number;
      }[];
    };
    pile: {
      total_events: number;
      tags: {
        tag: string;
        nevents: number;
      }[];
    };
  };
}
@Component({
  selector: 'app-mc-subcampaigns-stat',
  standalone: true,
  imports: [
    AsyncPipe,
    JsonPipe,
    MatProgressSpinnerModule,
    NgIf,
    MatExpansionModule,
    NgTemplateOutlet,
    BillionPipe
  ],
  templateUrl: './mc-subcampaigns-stat.component.html',
  styleUrl: './mc-subcampaigns-stat.component.css'
})
export class McSubcampaignsStatComponent {

  MCSubCampaignStats$: Observable<MCSubCampaignStatsInterface[]> = this.taskService.getMCSubCampaignStats().pipe(map((stats: MCSubCampaignStats[]) => {
    return stats.map((stat: MCSubCampaignStats) => {
      // Count total events from all tags per mc_subcampaign
      const evgen = {total_events:  (stat.stats.evgen.map((x) => x.nevents)).reduce((a, b) => a + b, 0),
        tags: stat.stats.evgen.sort((a, b) => b.nevents - a.nevents)};
      const simul = {total_events: (stat.stats.simul.map((x) => x.nevents)).reduce((a, b) => a + b, 0),
        fastsim_events: (stat.stats.simul.filter((x) => x.tag.startsWith('a')).map((x) => x.nevents)).reduce((a, b) => a + b, 0),
        fullsim_events: (stat.stats.simul.filter((x) => x.tag.startsWith('s')).map((x) => x.nevents)).reduce((a, b) => a + b, 0),
        tags: stat.stats.simul.sort((a, b) => b.nevents - a.nevents)};
      const pile = {total_events: (stat.stats.pile.filter((x) => x.tag.startsWith('r')).map((x) => x.nevents)).reduce((a, b) => a + b, 0),
        tags: stat.stats.pile.sort((a, b) => b.nevents - a.nevents)};
      return {
        mc_subcampaign: stat.mc_subcampaign,
        stats: {
          evgen,
          simul,
          pile
        }
      };
    });
  }), catchError((err) => {
      console.log(err);
      return [];
      }
  ));
  constructor(private taskService: TaskService) { }
}


