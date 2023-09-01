import {AfterViewInit, Component, OnInit} from '@angular/core';
import {MatLegacyTableDataSource as MatTableDataSource} from '@angular/material/legacy-table';
import {GroupProductionStats} from '../gp-stats/gp-stats';
import {ActivatedRoute, ParamMap, Router} from '@angular/router';
import {ViewportScroller} from '@angular/common';
import {StatsByOutputBase} from '../gp-stats/gp-stats.component';



export interface StatsByOutput{
      outputFormat: string;
      containers: number;
      size: number;
      containersToDelete: number;
      sizeToDelete: number;
}



@Component({
  selector: 'app-gp-stats-matrix',
  templateUrl: './gp-stats-matrix.component.html',
  styleUrls: ['./gp-stats-matrix.component.css']
})
export class GpStatsMatrixComponent implements OnInit, AfterViewInit {
  gpStats: GroupProductionStats[];
  availableAMITags: string[] = [];
  statsByOutput: Map<string, Map<string, [number, number]>>;
  statsByOutputBases: StatsByOutputBase[] = [];
  formatsOnPage: string[] = [];
  dataType: string;
  statMatrix: any[] = [];
  displayColumns: string[] = [];
  hoveredAMITag = '';
  hoveredFormat = '';
  chosenFormat: string | null = '';
  showNumbers = 0;
  hoverTable = false;
  constructor(private route: ActivatedRoute, private router: Router, private viewportScroller: ViewportScroller, ) { }

  ngOnInit(): void {
    this.gpStats = this.route.snapshot.data.gpStats;
    this.route.queryParamMap.subscribe((paramMap: ParamMap) => {
    const URLdataType = paramMap.get('type');
    this.chosenFormat = paramMap.get('base');
    if (paramMap.get('show') === '1' ){
      this.showNumbers = 1;
    } else {
      this.showNumbers = 0;
    }
    console.log(this.chosenFormat);
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

  private getBaseFormat(outputFormat: string): string{
    if (outputFormat.indexOf('_') > -1){
      return outputFormat.split('_')[1].replace(/\d/, '#').split('#')[0];
    } else {
      return outputFormat.replace(/\d/, '#').split('#')[0];
    }
  }

  private fetchData(isReal: boolean): void {
    this.availableAMITags = [];
    const allAvalaibleAMITags: string[] = [];
    this.displayColumns = [];
    this.statMatrix = [];
    for (const currentStat of this.gpStats){
      const baseFormat = this.getBaseFormat(currentStat.output_format);
      if ((currentStat.real_data === isReal) && ((this.chosenFormat === null) || (this.chosenFormat === baseFormat))){
        if ((currentStat.to_delete_containers > 0) && (this.availableAMITags.indexOf(currentStat.ami_tag) === -1)) {
          allAvalaibleAMITags.push(currentStat.ami_tag);
        }
      }
    }
    if (this.statsByOutputBases !== undefined){
      this.statsByOutputBases = [];
    }
    this.statsByOutput = new Map<string, Map<string, [number, number]>>();
    this.formatsOnPage = [];
    for (const currentStat of this.gpStats) {
      const baseFormat =  this.getBaseFormat(currentStat.output_format);
      if ((currentStat.real_data === isReal) && ((this.chosenFormat === null) || (this.chosenFormat === baseFormat))) {
        const base = currentStat.output_format.split('_')[1];
        if (this.statsByOutput.get(base) === undefined) {
          this.statsByOutput.set(base, new Map<string, [number, number]>());
          for (const amiTag of allAvalaibleAMITags){
            this.statsByOutput.get(base).set(amiTag, [0, 0]);
          }
        }
        if (currentStat.to_delete_containers > 0) {
          if (this.formatsOnPage.indexOf(base) === -1){
            this.formatsOnPage.push(base);
          }
          if (this.availableAMITags.indexOf(currentStat.ami_tag) === -1){
            this.availableAMITags.push(currentStat.ami_tag);
          }
          const currentValueDatasets = this.statsByOutput.get(base).get(currentStat.ami_tag)[0];
          const currentValueSizes = this.statsByOutput.get(base).get(currentStat.ami_tag)[1];
          this.statsByOutput.get(base).set(currentStat.ami_tag, [ currentValueDatasets + Number(currentStat.to_delete_containers),
          currentValueSizes + Number(currentStat.to_delete_size)]);
        }

      }

    }
    this.availableAMITags.sort();
    this.availableAMITags.reverse();
    this.displayColumns.push('firstColumn');
    for (const amiTag of this.availableAMITags){
      this.displayColumns.push(amiTag);
    }
    this.formatsOnPage.sort();
    for (const formatBase of this.formatsOnPage){
      const currentRow = {firstColumn: ''};
      currentRow.firstColumn = formatBase;
      for (const amiTag of this.availableAMITags){
        currentRow[amiTag] = this.statsByOutput.get(formatBase).get(amiTag);
      }
      this.statMatrix.push(currentRow);
    }
  }

  changeType(): void {
    this.router.navigate(['/gp-stats-matrix'],
      { queryParams: {type: this.dataType, show: this.showNumbers}, queryParamsHandling: 'merge' });
  }

  highlightRowAndColumn(amiTag: string, formatBase: string): void {
    this.hoveredAMITag = amiTag;
    this.hoveredFormat = formatBase;
    this.hoverTable = true;
  }


}
