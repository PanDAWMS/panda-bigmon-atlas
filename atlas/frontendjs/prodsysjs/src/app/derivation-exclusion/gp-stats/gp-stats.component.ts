import { AfterViewInit, Component, OnInit, inject } from '@angular/core';
import {ActivatedRoute, ParamMap, Router} from '@angular/router';
import {GroupProductionStats} from './gp-stats';
import {MatTableDataSource} from '@angular/material/table';
import {ViewportScroller} from '@angular/common';
import {GPStatsService} from "./gp-stats.service";
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
    styleUrls: ['./gp-stats.component.css'],
    standalone: false
})
export class GpStatsComponent implements OnInit, AfterViewInit {
  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private viewportScroller = inject(ViewportScroller);
  private gpStateService = inject(GPStatsService);
  gpStats: GroupProductionStats[];
  statsByOutput: Map<string, Map<string, StatsByOutput>>;
  statsByOutputBases: StatsByOutputBase[] = [];
  formatsOnPage: string[] = [];
  dataType: string;
  showXAxis = true;
  showYAxis = true;
  gradient = false;
  showLegend = true;
  showXAxisLabel = true;
  xAxisLabel = 'Format';
  showYAxisLabel = true;
  yAxisLabel = 'Size (TB)';
  view: any[] = [1800, 1000];
  sizeChart: any[] = [];
  sizeChartData: any[] = [];
  sortBySizeToDelete = true;
  selectedFormats: Record<string, boolean> = {};
  totalDatasets = 0;
  totalSize = 0;
  totalDatasetsToDelete = 0;
  totalSizeToDelete = 0;
  lastUpdateTime = '';
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
    const totalFormats = this.formatsOnPage.length;
    console.log(`Total formats on page: ${totalFormats}`);
    for (const formatBase of this.formatsOnPage){
      const currentDataSource = new MatTableDataSource<StatsByOutput>();
      const statsForBase: StatsByOutput[] = [];
      for (const statsForFormat of this.statsByOutput[formatBase].values()){
        let currentStat = {};
        statsForBase.push(statsForFormat);
        this.totalDatasets += statsForFormat.containers;
        this.totalSize += statsForFormat.size ;
        this.totalDatasetsToDelete += statsForFormat.containersToDelete;
        this.totalSizeToDelete  += statsForFormat.sizeToDelete;
        if (statsForFormat.sizeToDelete > 0) {
          currentStat = {name: statsForFormat.outputFormat,
            series: [{ name: 'For deletion', value: Number(statsForFormat.sizeToDelete) / 1e12 },
              {name: 'Superseded', value: Number(statsForFormat.size) / 1e12 }]};
          this.sizeChartData.push(currentStat);
        }
      }
      statsForBase.sort((a, b) => a.outputFormat.localeCompare(b.outputFormat));
      currentDataSource.data = statsForBase;
      this.statsByOutputBases.push({outputFormatBase: formatBase, dataSource: currentDataSource});
    }
    // Initialise format checkboxes — preserve user selection if format set unchanged
    const existingKeys = Object.keys(this.selectedFormats);
    const newKeys = this.formatsOnPage;
    if (existingKeys.length === 0 || existingKeys.some(k => !newKeys.includes(k))) {
      this.selectedFormats = {};
      for (const f of this.formatsOnPage) { this.selectedFormats[f] = true; }
    }
    this.updateChart();
  }
  updateChart(): void {
    const filtered = this.sizeChartData.filter(item => {
      const base = (item.name as string).split('_')[1]?.replace(/\d/, '#').split('#')[0] ?? item.name;
      return this.selectedFormats[base] ?? true;
    });
    filtered.sort((a, b) => {
      const idx = this.sortBySizeToDelete ? 0 : 1;
      return b.series[idx].value - a.series[idx].value;
    });
    this.sizeChart = [...filtered];
  }
  changeType(): void {
    this.router.navigate(['/gp-stats'], { queryParams: {type: this.dataType} });
  }
  onChartSelect(event): void{
    this.router.navigate(['/gp-deletion', this.dataType,  event.series]);
  }
}
