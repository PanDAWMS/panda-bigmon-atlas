import {
  AfterContentChecked,
  AfterContentInit,
  AfterViewChecked,
  AfterViewInit,
  Component,
  OnInit,
  ViewChild
} from '@angular/core';
import { Router, ActivatedRoute, ParamMap } from '@angular/router';
import {animate, state, style, transition, trigger} from '@angular/animations';

import {GroupProductionDeletionContainer} from './gp-deletion-container';
import {GPDeletionContainerService} from './gp-deleation.service';
import {SelectionModel} from '@angular/cdk/collections';
import {MatTableDataSource} from '@angular/material/table';
import {ViewportScroller} from '@angular/common';
import {GpContainerDetailsService} from './gp-container-details.service';


export interface ContainerByTag{
      amiTag: string;
      dataSource: MatTableDataSource<GroupProductionDeletionContainer>;
      selection: SelectionModel<GroupProductionDeletionContainer>;
}


@Component({
  selector: 'app-derivation-exclusion',
  templateUrl: './derivation-exclusion.component.html',
  styleUrls: ['./derivation-exclusion.component.css'],
  animations: [
    trigger('detailExpand', [
      state('collapsed', style({height: '0px', minHeight: '0'})),
      state('expanded', style({height: '*'})),
      transition('expanded <=> collapsed', animate('225ms cubic-bezier(0.4, 0.0, 0.2, 1)')),
    ]),
  ],
})



export class DerivationExclusionComponent implements OnInit, AfterViewInit{
  gpList: GroupProductionDeletionContainer[];
  outputType: string;
  currentFragment: string;
  dataType: string;
  extendMessage = '';
  totalSelected = 0;
  totalSelectedSize = 0;
  columnsToDisplay = [ 'select', 'container', 'available_tags', 'age', 'extended_till'];
  containersByTag = new Map<string, GroupProductionDeletionContainer[]>();
  // selection = new SelectionModel<GroupProductionDeletionContainer>(true, []);
  containersByTagTables: Array<ContainerByTag> = [];
  expandedElement: GroupProductionDeletionContainer| null;
  selectedContainerDetails: GroupProductionDeletionContainer[] = [];
  selectedContainerDetailsDataSource: MatTableDataSource<GroupProductionDeletionContainer>;
  extendNumbers: number;
  opened: boolean;


  constructor(private route: ActivatedRoute, private gpDeletionContainerService: GPDeletionContainerService, private router: Router,
              private viewportScroller: ViewportScroller, private gpContainerDetailsService: GpContainerDetailsService) { }

  ngOnInit(): void {
    this.outputType = this.route.snapshot.paramMap.get('output');
    this.dataType = this.route.snapshot.paramMap.get('data_type');
    this.route.fragment.subscribe(params => {
        this.currentFragment = params;
      });
    this.getContainers();


  }
    /** Whether the number of selected elements matches the total number of rows. */
  isAllSelected(dataSource: MatTableDataSource<GroupProductionDeletionContainer>,
                selection: SelectionModel<GroupProductionDeletionContainer>): boolean {
    const numSelected = selection.selected.length;
    const numRows = dataSource.data.length;
    return numSelected === numRows;
  }

  /** Selects all rows if they are not all selected; otherwise clear selection. */
  masterToggle(dataSource: MatTableDataSource<GroupProductionDeletionContainer>,
               selection: SelectionModel<GroupProductionDeletionContainer>): void {
    this.isAllSelected(dataSource, selection) ?
        selection.clear() :
        dataSource.data.forEach(row => selection.select(row));
    this.recountSelected();
  }
  selectRowWithCounting(selection: SelectionModel<GroupProductionDeletionContainer>, row: GroupProductionDeletionContainer): void{
    selection.toggle(row);
    this.recountSelected();
  }
  recountSelected(): void{
    this.totalSelected = 0;
    this.totalSelectedSize = 0;
    for (const GPAMIContainers of this.containersByTagTables) {
       GPAMIContainers.dataSource.data.forEach(row => {
          if (GPAMIContainers.selection.isSelected(row)){
            this.totalSelected += 1;
            this.totalSelectedSize = this.totalSelectedSize + Number(row.size);
          }
        });
      }
  }
  /** The label for the checkbox on the passed row */
  checkboxLabel(dataSource: MatTableDataSource<GroupProductionDeletionContainer>,
                selection: SelectionModel<GroupProductionDeletionContainer>, row?: GroupProductionDeletionContainer): string {
    if (!row) {
      return `${this.isAllSelected(dataSource, selection) ? 'select' : 'deselect'} all`;
    }
    return `${selection.isSelected(row) ? 'deselect' : 'select'} row ${row.id}`;
  }

  ngAfterViewInit(): void {
        setTimeout(() => this.viewportScroller.scrollToAnchor(this.currentFragment), 100);
   }

  getContainers(): void{
      this.gpList = this.route.snapshot.data.gpList;
      this.fillTablesByTag();
  }
  fillTablesByTag(): void{
    for (const container of this.gpList) {
      if (this.containersByTag.has(container.ami_tag)) {
        const currentContainer = this.containersByTag.get(container.ami_tag);
        currentContainer.push(container);
        this.containersByTag.set(container.ami_tag, currentContainer);
      } else {
        this.containersByTag.set(container.ami_tag, [container]);
      }
    }

    for (const [amiTag, containerByTag] of this.containersByTag.entries()){
      const dataSource = new MatTableDataSource<GroupProductionDeletionContainer>();
      dataSource.data = containerByTag;
      const selection =  new SelectionModel<GroupProductionDeletionContainer>(true, []);
      this.containersByTagTables.push({amiTag, dataSource, selection});
    }

    this.route.fragment.subscribe(f => {
          this.viewportScroller.scrollToAnchor(f);
      });
  }

  extend(row: GroupProductionDeletionContainer): void {
    if (row === this.expandedElement) {
      this.expandedElement = null;
    } else {
      this.expandedElement = row;
    }
    this.selectedContainerDetails = [];
    this.selectedContainerDetailsDataSource = new MatTableDataSource<GroupProductionDeletionContainer>();

    this.gpContainerDetailsService.getGPContainerDetails(row.id.toString()).subscribe(details => {
      for (const GPcontainer of details.containers) {
        this.selectedContainerDetails.push(GPcontainer);
      }
      this.selectedContainerDetailsDataSource.data = this.selectedContainerDetails;
    });
    this.router.navigate(['.'], { fragment: row.id.toString(), queryParamsHandling: 'preserve', relativeTo: this.route });
  }

  sendExtension(): void {
    const selectedContainers: GroupProductionDeletionContainer[] = [];
    for (const GPAMIContainers of this.containersByTagTables) {
       GPAMIContainers.dataSource.data.forEach(row => {
          if (GPAMIContainers.selection.isSelected(row)){
            selectedContainers.push(row);
          }
        });
      }
    this.gpDeletionContainerService.askExtension({message: this.extendMessage,
      containers: selectedContainers, number_of_extensions: 0}).subscribe();
  }
}
