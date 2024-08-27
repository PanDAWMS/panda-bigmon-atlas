import {Component, computed, effect, inject, Input, OnInit, signal, ViewChild} from '@angular/core';
import {
  ChartComponent, NgApexchartsModule,
  ApexAxisChartSeries,
  ApexChart,
  ApexPlotOptions,
  ApexXAxis, ApexYAxis, ApexTooltip, ApexLegend
} from "ng-apexcharts";
import {StageProfileSpans, TaskStageProfile, TaskStageProfileService} from "./task-stage-profile.service";
import {JsonPipe} from "@angular/common";
import {MatProgressSpinner} from "@angular/material/progress-spinner";
import {ActivatedRoute} from "@angular/router";

export type ChartOptions = {
  series: ApexAxisChartSeries;
  chart: ApexChart;
  xaxis: ApexXAxis;
  yaxis: ApexYAxis;
  plotOptions: ApexPlotOptions;
  tooltip: ApexTooltip;
  animations: any;
  legend: ApexLegend;
};

@Component({
  selector: 'app-task-stage-profile',
  standalone: true,
  imports: [
    NgApexchartsModule,
    JsonPipe,
    MatProgressSpinner
  ],
  templateUrl: './task-stage-profile.component.html',
  styleUrl: './task-stage-profile.component.css'
})
export class TaskStageProfileComponent implements OnInit {
  @ViewChild("chart") chart: ChartComponent;

  taskStageProfileService = inject( TaskStageProfileService);
  public stageProfile = signal< Omit<TaskStageProfile, 'spans'>>( {
  done_attempts: [],
  failed_attempts_after_success: 0,
  done_attempts_after_success: 0,
  files_staged: 0,
  task: 0,
  dataset: '',
  source: '',
  date_since: '',
  date_until: '',
  total_files: 0,
  total_attempts: 0
});

  @Input() set taskID(value: string) {
    this.taskPageID = value;

  }
  taskPageID = '';
  errorMessage = computed(() => this.taskStageProfileService.error$());
  public chartOptions: Partial<ChartOptions>;

    constructor(private route: ActivatedRoute) {
    this.chartOptions = {
      series: [
        {
          data: []
        }
        ],
      chart: {
        height: 900,
        type: "rangeBar",
        animations: {
          enabled: false,
        },
        selection: {
          enabled: false,
        },
        toolbar: {
          show: false,
        },
        zoom: {
          enabled: false,
        },
        redrawOnWindowResize: false,
      },
      plotOptions: {
        bar: {
          horizontal: true,
        },
      },
      xaxis: {
        type: "datetime",
      },
      yaxis: {
        show: false,
        labels: {
          show: false,
        }
      },
      // tooltip: {
      //   enabled: false,
      // },
      legend: {
        customLegendItems: ['created_at-submitted_at transfer-done', 'submitted_at-transferred_at transfer-done',
          'created_at-transferred_at transfer-failed', 'created_at-transferred_at transfer-done repeated',
        'created_at-transferred_at transfer-failed repeated'],
        labels: {
          colors: ['#f7dc6f', '#00E396', '#FF4560', '#775DD0', '#6c3483'],
        },
        onItemClick: {
            toggleDataSeries: false
          },
          onItemHover: {
              highlightDataSeries: false
          },
      }

    };

  }
  ngOnInit() {
    this.route.queryParams.subscribe(params => {
      if ('dataset' in params && 'source' in params) {
        console.log(params);
        this.taskStageProfileService.getTaskStageProfile(this.taskPageID, params.dataset, params.source).subscribe((data) => {
          this.stageProfile.set(data);
          this.chartOptions.series = [{
            data: data.spans
          },{data:[]},{data:[]},{data:[]},{data:[]}];
        });
      } else {
        this.taskStageProfileService.getTaskStageProfile(this.taskPageID).subscribe((data) => {
          this.stageProfile.set(data);
          this.chartOptions.series = [{
            data: data.spans
          },{data:[]},{data:[]},{data:[]},{data:[]}];
        });
      }
    });
  }

}
