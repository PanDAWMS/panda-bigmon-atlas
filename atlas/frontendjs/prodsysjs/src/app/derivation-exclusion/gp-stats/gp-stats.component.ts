import {AfterViewInit, Component, OnInit} from '@angular/core';
import {ActivatedRoute, ParamMap, Router} from '@angular/router';
import {GroupProductionStats} from './gp-stats';
import {MatTableDataSource} from '@angular/material/table';
import {GroupProductionDeletionContainer} from '../gp-deletion-container';
import {SelectionModel} from '@angular/cdk/collections';
import {ViewportScroller} from '@angular/common';




const ALL_FORMATS = ['BPHY', 'EGAM', 'EXOT', 'FTAG', 'HDBS', 'HIGG', 'HION', 'JETM', 'LCALO', 'MUON', 'PHYS',
                'STDM', 'SUSY', 'TAUP', 'TCAL', 'TOPQ', 'TRIG', 'TRUTH'];

export interface StatsByOutput{
      outputFormat: string;
      containers: number;
      size: number;
      containersToDelete: number;
      sizeToDelete: number;
}

export interface StatsByOutputBase{
      outputFormatBase: string;
      dataSource: MatTableDataSource<StatsByOutput>;
}

@Component({
  selector: 'app-gp-stats',
  templateUrl: './gp-stats.component.html',
  styleUrls: ['./gp-stats.component.css']
})
export class GpStatsComponent implements OnInit, AfterViewInit {
  gpStats: GroupProductionStats[];
  statsByOutput: Map<string, Map<string, StatsByOutput>>;
  statsByOutputBases: StatsByOutputBase[] = [];
  formatsOnPage: string[] = [];
  dataType: string;
  activeMC: string;
  activeData: string;
  constructor(private route: ActivatedRoute, private router: Router, private viewportScroller: ViewportScroller, ) { }

  ngOnInit(): void {
    this.gpStats = this.route.snapshot.data.gpStats;
    this.route.queryParamMap.subscribe((paramMap: ParamMap) => {
    const URLdataType = paramMap.get('type');
    if (URLdataType === 'data'){
      this.fetchData(true);
      this.activeMC = '';
      this.activeData = 'primary';
      this.dataType = 'data';
    } else {
      this.fetchData(false);
      this.activeMC = 'primary';
      this.activeData = '';
      this.dataType = 'mc';
    }

  });

  }
  ngAfterViewInit(): void {
    this.route.fragment.subscribe(f => {
      this.viewportScroller.scrollToAnchor(f);
    });
  }

  private fetchData(isReal: boolean): void {
    if (this.statsByOutputBases !== undefined){
      this.statsByOutputBases = [];
    }
    this.statsByOutput = new Map<string, Map<string, StatsByOutput>>();
    this.formatsOnPage = [];
    for (const currentStat of this.gpStats){
      if (currentStat.real_data === isReal){
        const base = currentStat.output_format.split('_')[1].replace(/\d/, '#').split('#')[0];
        if (this.statsByOutput[base] === undefined ){
            this.statsByOutput[base] = new Map<string, StatsByOutput>();
            this.formatsOnPage.push(base);
        }
        if ( this.statsByOutput[base].get(currentStat.output_format) === undefined){
          this.statsByOutput[base].set(currentStat.output_format, {outputFormat: currentStat.output_format, containers: 0, size: 0,
          containersToDelete:  0, sizeToDelete:  0} );
      }
        this.statsByOutput[base].get(currentStat.output_format).containers += Number(currentStat.containers);
        this.statsByOutput[base].get(currentStat.output_format).size += Number(currentStat.size);
        this.statsByOutput[base].get(currentStat.output_format).containersToDelete += Number(currentStat.to_delete_containers);
        this.statsByOutput[base].get(currentStat.output_format).sizeToDelete += Number(currentStat.to_delete_size);
      }

    }
    this.formatsOnPage.sort();
    for (const formatBase of this.formatsOnPage){
      const currentDataSource = new MatTableDataSource<StatsByOutput>();
      const statsForBase: StatsByOutput[] = [];

      for (const statsForFormat of this.statsByOutput[formatBase].values()){
        statsForBase.push(statsForFormat);
      }
      statsForBase.sort((a, b) => a.outputFormat.localeCompare(b.outputFormat));
      currentDataSource.data = statsForBase;
      this.statsByOutputBases.push({outputFormatBase: formatBase, dataSource: currentDataSource});
    }
  }
}
