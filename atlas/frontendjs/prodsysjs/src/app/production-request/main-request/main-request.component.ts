import {AfterViewInit, ChangeDetectionStrategy, Component, OnInit, signal, Signal, ViewChild} from '@angular/core';
import {PatternSelectionComponent} from '../pattern-selection/pattern-selection.component';
import {CampaignPattern} from '../production-request-models';
import {CdkVirtualForOf, CdkVirtualScrollViewport, ScrollingModule} from "@angular/cdk/scrolling";
import {DataSource} from "@angular/cdk/collections";
import {MatTableDataSource} from "@angular/material/table";
import {FormsModule} from "@angular/forms";
import {toSignal} from "@angular/core/rxjs-interop";




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
  rows: ColumnsWihtCheckbox[] = this.createMockRows();
  rowsDatasource: MatTableDataSource<ColumnsWihtCheckbox>;
  scrollIndex = 0;
  filterString = '';
  currentIndex = signal(0);
  ngOnInit() {
    this.rowsDatasource = new MatTableDataSource<ColumnsWihtCheckbox>(this.rows);
  }

  createMockRows(): ColumnsWihtCheckbox[] {
    const mockRows: ColumnsWihtCheckbox[] = [];
    const maxColumns = 10;
    for (let i = 0; i < 150000; i++) {
      const columns: Column[] = [];
      let usedColumns = 0;
      for (let j = 0; j < 10; j++) {
        let randomSpan = Math.floor(Math.random() * 4) + 1;
        if (usedColumns + randomSpan > maxColumns) {
          randomSpan = maxColumns - usedColumns;
        }
        if (randomSpan === 0) {
          break;
        }

        const randomBackgroundColor = ["bg-red-200", "bg-green-200", "bg-blue-200", "bg-yellow-200"][Math.floor(Math.random() * 4)];
        columns.push({
          text: `Row ${i}, Col ${j}`,
          span: randomSpan,
          cssClasses: `border ${randomBackgroundColor}`,

        });
      }
      const checkboxrandom = Math.floor(Math.random() * 3);
      let checkbox = false;
      if (checkboxrandom === 1) {
        checkbox = true;
      }
      if (checkboxrandom === 0) {
        checkbox = undefined;
      }
      mockRows.push({checkbox, columns, index: i});
    }
    return mockRows;
  }


  handleColumnClick(rowIndex, colIndex) {
    console.log(rowIndex, colIndex);
  }

  handleCheckboxChange(rowIndex) {
    console.log(rowIndex);
  }

  reShuffle() {
    shuffle(this.rows);
    this.rowsDatasource.data = this.rows;
  }

  scrollTo(index: number) {
    if (index === -1){
      this.scrollViewport.scrollToIndex(this.rows.length - 1);
      return;
    }
    if (index === -2){
      this.scrollViewport.scrollTo({end: 0});
      return;
    }
    this.scrollViewport.scrollToIndex(this.scrollIndex);
  }

  filterRows() {
    const currentRealIndex = this.rowsDatasource.filteredData[this.currentIndex()].index;
    this.rowsDatasource.filter = this.filterString.trim().toLowerCase();
    console.log(currentRealIndex, this.rowsDatasource.filteredData.indexOf(this.rows[currentRealIndex]));
    if (this.rowsDatasource.filteredData.indexOf(this.rows[currentRealIndex]) !== -1) {
      this.scrollViewport.scrollToIndex(this.rowsDatasource.filteredData.indexOf(this.rows[currentRealIndex]));
    }
  }

  ngAfterViewInit(): void {
    this.scrollViewport.scrolledIndexChange.subscribe((index) => {
      this.currentIndex.set(index);
    });

  }
}
export interface Column {
  text: string;        // The text content of the column cell
  span: number;        // The number of column spans (e.g., 1, 2, or 4)
  cssClasses: string;  // Tailwind CSS classes for styling
}

export interface ColumnsWihtCheckbox  {
  checkbox: boolean | undefined; // The checkbox value
  index: number;
  columns: Column[];             // The columns in the row
}

function shuffle(array) {
  let currentIndex = array.length;

  // While there remain elements to shuffle...
  while (currentIndex != 0) {

    // Pick a remaining element...
    let randomIndex = Math.floor(Math.random() * currentIndex);
    currentIndex--;

    // And swap it with the current element.
    [array[currentIndex], array[randomIndex]] = [
      array[randomIndex], array[currentIndex]];
  }
}
