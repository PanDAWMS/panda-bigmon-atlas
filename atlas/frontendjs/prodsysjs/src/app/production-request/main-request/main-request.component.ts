import {AfterViewInit, ChangeDetectionStrategy, Component, inject, OnInit, signal, ViewChild} from '@angular/core';
import {PatternSelectionComponent} from '../pattern-selection/pattern-selection.component';
import {CampaignPattern, ProductionRequests, ProductionTask, Slice} from '../production-request-models';
import {CdkVirtualScrollViewport, ScrollingModule} from '@angular/cdk/scrolling';
import {MatTableDataSource} from '@angular/material/table';
import {FormsModule} from '@angular/forms';
import {ProductionRequestService} from '../production-request.service';
import {Router} from "@angular/router";


@Component({
  selector: 'app-main-request',
  imports: [
    PatternSelectionComponent,
    ScrollingModule,
    FormsModule
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './main-request.component.html',
  styleUrl: './main-request.component.css'
})
export class MainRequestComponent implements OnInit, AfterViewInit {

  @ViewChild('scrollViewport') scrollViewport: CdkVirtualScrollViewport;

  productionRequestService = inject(ProductionRequestService);
  router = inject(Router);
  mockPatterns: CampaignPattern[] = [
    {
      campaign: 'MC23',
      subcampaing: 'MC23a',
      project: 'mc21_13p6TeV',
      patterns: [
        {
          id: 1,
          pattern: 'Pattern 1',
          steps: [
            {step: 'Evgen', tag: 'e1234'},
            {step: 'Evgen Merge', tag: 'e1235'}
          ]
        }
      ]
    },
    {
      campaign: 'MC23',
      subcampaing: 'MC23c',
      project: 'mc23_13p6TeV',
      patterns: [
        {
          id: 2,
          pattern: 'Pattern 2',
          steps: [
            {step: 'Simul', tag: 's1236'},
            {step: 'Merge', tag: 's1237'}
          ]
        }
      ]
    },
    {
      campaign: 'MC23',
      subcampaing: 'MC23d',
      project: 'mc23_13p6TeV',
      patterns: [
        {
          id: 3,
          pattern: 'Pattern 3',
          steps: [
            {step: 'Digi', tag: 'd1238'},
            {step: 'Reco', tag: 'r1239'},
            {step: 'Rec Merge', tag: 'r1240'},
            {step: 'Deriv Merge', tag: 'd1241'}
          ]
        }
      ]
    }
  ];
  rowsDatasource: MatTableDataSource<ProdReqColumnsWithCheckbox>;
  indexMap: Map<number, number> = new Map<number, number>();
  renderedSlicesMap: Map<number, ProdReqColumnsWithCheckbox[]> = new Map<number, ProdReqColumnsWithCheckbox[]>();
  scrollIndex = 0;
  filterString = '';
  currentIndex = signal(0);
  productionRequests: ProductionRequests;
  ngOnInit() {
    this.rowsDatasource = new MatTableDataSource<ProdReqColumnsWithCheckbox>([]);
    this.productionRequestService.getSteps('62407').subscribe( resultResponse => {
      this.productionRequests = resultResponse;
      const existingSteps: string[] = [];
      for (const slice of this.productionRequests.slices){
        if (slice.steps.length > 0){
          for (const step of slice.steps){
            if (!existingSteps.includes(step.step_name)){
              existingSteps.push(step.step_name);
            }
          }
        }
      }
      const rows: ProdReqColumnsWithCheckbox[] = [];
      let currentOffset = 0;
      for (const slice of this.productionRequests.slices){
        this.indexMap.set(slice.slice, currentOffset);
        const renderedSlices = renderSlice(slice, existingSteps);
        currentOffset += renderedSlices.length;
        this.renderedSlicesMap.set(slice.slice, renderedSlices);
        rows.push(...renderedSlices);
      }
            //bottom offset rows
      const offsetRow: ProdReqColumnsWithCheckbox = {
        checkbox: false,
        columns: [],
        sliceNumber: -9999,
        indexInsideSlice: -9999
      };
      rows.push(offsetRow);
      rows.push(offsetRow);
      rows.push(offsetRow);
      this.rowsDatasource.data = rows;
    });
  }



  handleColumnClick(sliceIndex: number, insideSliceIndex: number, colIndex: number) {
    console.log(sliceIndex, insideSliceIndex, colIndex);
    const cell = this.renderedSlicesMap.get(sliceIndex)[insideSliceIndex].columns[colIndex];
    switch (cell.cellType){
      case 'task':
        this.router.navigate(['task', cell.value]);
        break;
      case 'step':
        console.log('step');
        break;
      case 'input_data':
        console.log('input_data');
        break;
      case 'dataset':
        console.log('dataset');
        break;
      case 'comment':
        console.log('comment');
        break;
      case 'status':
        console.log('status');
        break;
      case 'empty':
        console.log('empty');
        break;
    }
  }

  handleCheckboxChange(sliceIndex) {
    console.log(sliceIndex);
  }



  scrollTo(index: number) {
    if (index === -1){
      this.scrollViewport.scrollToIndex(this.rowsDatasource.data.length - 1);
      return;
    }
    if (index === -2){
      this.scrollViewport.scrollTo({end: 0});
      return;
    }
    if (this.indexMap.has(index)){
      this.scrollViewport.scrollToIndex(this.indexMap.get(index));
    }

  }

  filterRows() {
    const filteredRows: ProdReqColumnsWithCheckbox[] = [];
    this.indexMap = new Map<number, number>();
    for (const [sliceNumber, renderedSlices] of this.renderedSlicesMap.entries()){
      if (renderedSlices !== undefined){
        // Check if some column of the first row contains the filter string
        if (renderedSlices[0].columns.some(column => column.text.toLowerCase().includes(this.filterString.toLowerCase()))) {
          filteredRows.push(...renderedSlices);
          this.indexMap.set(sliceNumber, filteredRows.length);
        }
      }
    }
    this.rowsDatasource.data = filteredRows;
  }

  ngAfterViewInit(): void {
    this.scrollViewport.scrolledIndexChange.subscribe((index) => {
      this.currentIndex.set(index);
    });

  }
}
export interface ProdReqColumn {
  text: string;        // The text content of the column cell
  span: number;        // The number of column spans (e.g., 1, 2, or 4)
  cssClasses: string;  // Tailwind CSS classes for styling
  cellType: 'task'|'step'|'input_data'|'dataset'|'comment'|'status'|'empty'; // The type of the cell
  value?: string; // The value of the cell (optional)
}

export interface ProdReqColumnsWithCheckbox {
  checkbox: boolean | undefined; // The checkbox value
  sliceNumber: number;
  indexInsideSlice: number;
  columns: ProdReqColumn[];             // The columns in the row
}




function renderSlice(slice: Slice, existingSteps: string[]): ProdReqColumnsWithCheckbox[] {
    const columnNumbers = existingSteps.length + 2;
    const rows: ProdReqColumnsWithCheckbox[] = [];
    // first row
    const firstRow: ProdReqColumnsWithCheckbox = {
      checkbox: true,
      columns: [],
      sliceNumber: slice.slice,
      indexInsideSlice: 0
    };
    const firstRowColumns: ProdReqColumn[] = [];
    if (slice.input_data && !slice.dataset) {
      firstRowColumns.push({text: slice.input_data, span: columnNumbers - 2, cssClasses: '', cellType: "input_data"});
      firstRowColumns.push({text: '', span: 2, cssClasses: '', cellType: "empty"});
    } else if (slice.dataset && !slice.input_data) {
      firstRowColumns.push({text: slice.dataset, span: columnNumbers - 2, cssClasses: '', cellType: "dataset"});
      firstRowColumns.push({text: '', span: 2, cssClasses: '', cellType: "empty"});
    } else {
      firstRowColumns.push({text: slice.input_data, span: columnNumbers / 2, cssClasses: '', cellType: "input_data"});
      firstRowColumns.push({text: slice.dataset, span: columnNumbers - columnNumbers / 2, cssClasses: '', cellType: "dataset"});
    }
    firstRow.columns = firstRowColumns;
    rows.push(firstRow);
    // second row
    const secondRow: ProdReqColumnsWithCheckbox = {
      checkbox: false,
      columns: [],
      sliceNumber: slice.slice,
      indexInsideSlice: 1
    };
    const secondRowColumns: ProdReqColumn[] = [];
    secondRowColumns.push({text: slice.comment, span: columnNumbers - 2, cssClasses: '', cellType: "comment"});
    secondRowColumns.push({text: 'events:', span: 1, cssClasses: '', cellType: "empty"});
    secondRowColumns.push({text: slice.input_events.toString(), span: 1, cssClasses: '', cellType: "empty"});
    secondRow.columns = secondRowColumns;
    rows.push(secondRow);
    // step tag row
    const stepRow: ProdReqColumnsWithCheckbox = {
      checkbox: false,
      columns: [],
      sliceNumber: slice.slice,
      indexInsideSlice: 2
    };
    const stepRowColumns: ProdReqColumn[] = [];
    const taskPerStep: ProductionTask[][] = new Array(existingSteps.length);
    const stepsInSlice = slice.steps.length;
    let maxTasksLength = 0;
    for (let i = 0, j = 0; i < existingSteps.length; i++) {
      if (j >= stepsInSlice) {
        stepRowColumns.push({text: '', span: 1, cssClasses: '', cellType: "empty"});
        taskPerStep[i] = [];
        continue;
      }
      if (slice.steps[j].step_name === existingSteps[i]) {
           stepRowColumns.push({text: slice.steps[j].ami_tag, span: 1, cssClasses: '', cellType: "step"});
           taskPerStep[i] = slice.steps[j].tasks;
           if (slice.steps[j].tasks.length > maxTasksLength) {
             maxTasksLength = slice.steps[j].tasks.length;
           }
           j++;
      } else {
          stepRowColumns.push({text: '', span: 1, cssClasses: '', cellType: "empty"});
          taskPerStep[i] = [];
      }
    }
    stepRowColumns.push({text: 'Status 1', span: columnNumbers - existingSteps.length, cssClasses: '', cellType: "status"});
    stepRow.columns = stepRowColumns;
    rows.push(stepRow);
    for (let i = 0; i < maxTasksLength; i++) {
      const taskRow: ProdReqColumnsWithCheckbox = {
        checkbox: false,
        columns: [],
        sliceNumber: slice.slice,
        indexInsideSlice: 3 + i
      };
      const taskRowColumns: ProdReqColumn[] = [];
      for (let j = 0; j < existingSteps.length; j++) {
        if (taskPerStep[j][i]) {
          taskRowColumns.push({text: taskPerStep[j][i].status.toString(), span: 1, cssClasses: '',
            cellType: "task", value: taskPerStep[j][i].id.toString()});
        } else {
          taskRowColumns.push({text: '', span: 1, cssClasses: '', cellType: "empty"});
        }
      }
      if (i === 0){
        taskRowColumns.push({text: 'Status 2', span: columnNumbers - existingSteps.length, cssClasses: '', cellType: "status"});
      } else {
        taskRowColumns.push({text: '', span: columnNumbers - existingSteps.length, cssClasses: '', cellType: "empty"});
      }
      taskRow.columns = taskRowColumns;
      rows.push(taskRow);


    }
    return rows;
}
