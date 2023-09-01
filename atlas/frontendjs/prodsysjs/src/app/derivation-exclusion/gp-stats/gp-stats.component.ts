import {AfterViewInit, Component, OnInit} from '@angular/core';
import {ActivatedRoute, ParamMap, Router} from '@angular/router';
import {GroupProductionStats} from './gp-stats';
import {MatLegacyTableDataSource as MatTableDataSource} from '@angular/material/legacy-table';
import {GroupProductionDeletionContainer} from '../gp-deletion-container';
import {SelectionModel} from '@angular/cdk/collections';
import {ViewportScroller} from '@angular/common';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import {GPStatsService} from "./gp-stats.service";



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
  showXAxis = true;
  showYAxis = true;
  gradient = false;
  showLegend = false;
  showXAxisLabel = true;
  xAxisLabel = 'Format';
  showYAxisLabel = true;
  yAxisLabel = 'Size (TB)';
  view: any[] = [1800, 1000];
  sizeChart: any[] = [];
  sizeChartData: any[] = [];
  totalDatasets = 0;
  totalSize = 0;
  totalDatasetsToDelete = 0;
  totalSizeToDelete = 0;
  lastUpdateTime = '';

  constructor(private route: ActivatedRoute, private router: Router, private viewportScroller: ViewportScroller,
              private gpStateService: GPStatsService ) { }

  ngOnInit(): void {
    this.gpStateService.GPLastUpdateTime().subscribe(lastUpdateTime => this.lastUpdateTime = lastUpdateTime);
    this.gpStats = this.route.snapshot.data.gpStats;
    this.route.queryParamMap.subscribe((paramMap: ParamMap) => {
    const URLdataType = paramMap.get('type');
    if (URLdataType === 'data'){
      this.fetchData(true);
      this.dataType = 'data';
    } else {
      this.fetchData(false);
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
    this.sizeChartData = [];
    this.totalDatasets = 0;
    this.totalSize = 0;
    this.totalDatasetsToDelete = 0;
    this.totalSizeToDelete = 0;
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
        this.totalDatasets += statsForFormat.containers;
        this.totalSize += statsForFormat.size ;
        this.totalDatasetsToDelete += statsForFormat.containersToDelete;
        this.totalSizeToDelete  += statsForFormat.sizeToDelete;
        if (statsForFormat.sizeToDelete > 0) {
          this.sizeChartData.push({name: statsForFormat.outputFormat, value: Number(statsForFormat.sizeToDelete) / 1e12});
        }
      }
      statsForBase.sort((a, b) => a.outputFormat.localeCompare(b.outputFormat));
      currentDataSource.data = statsForBase;
      this.statsByOutputBases.push({outputFormatBase: formatBase, dataSource: currentDataSource});
    }
    this.sizeChartData.sort((a, b) => (a.value > b.value) ? -1 : ((b.value > a.value) ? 1 : 0));
    this.sizeChart = this.sizeChartData;
  }

  changeType(): void {
    // console.log(this.dataType);
    this.router.navigate(['/gp-stats'], { queryParams: {type: this.dataType} });
  }

  onChartSelect(event): void{
    this.router.navigate(['/gp-deletion', this.dataType,  event.name]);
  }
}
