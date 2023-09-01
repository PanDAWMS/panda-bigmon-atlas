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
import {AMITag, AmiTagService} from "./ami-tag.service";


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
  totalFiltered = 0;
  totalFilteredSize = 0;
  totalFilteredExpired = 0;
  totalFilteredExpiredSize = 0;
  columnsToDisplay = [ 'select', 'container', 'available_tags', 'age', 'extensions_number', 'extended_till'];
  containersByTag = new Map<string, GroupProductionDeletionContainer[]>();
  // selection = new SelectionModel<GroupProductionDeletionContainer>(true, []);
  containersByTagTables: Array<ContainerByTag> = [];
  expandedElement: GroupProductionDeletionContainer| null;
  selectedContainerDetails: GroupProductionDeletionContainer[] = [];
  selectedContainerDetailsDataSource: MatTableDataSource<GroupProductionDeletionContainer>;
  extendNumbers: number;
  opened: boolean;
  amiTagsDescription: Map<string, AMITag>;
  mainFilter = '';
  expiredFilter = '';
  sendClicked = false;

  constructor(private route: ActivatedRoute, private gpDeletionContainerService: GPDeletionContainerService, private router: Router,
              private viewportScroller: ViewportScroller, private gpContainerDetailsService: GpContainerDetailsService,
              private amiTagService: AmiTagService) { }

  ngOnInit(): void {
    this.outputType = this.route.snapshot.paramMap.get('output');
    this.dataType = this.route.snapshot.paramMap.get('data_type');
    this.route.queryParamMap.subscribe((paramMap: ParamMap) => {
      this.mainFilter = paramMap.get('main_filter');
      this.expiredFilter = paramMap.get('expired');
    });
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
        dataSource.filteredData.forEach(row => selection.select(row));
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
    this.containersByTag = new Map<string, GroupProductionDeletionContainer[]>();
    this.containersByTagTables = [];
    for (const container of this.gpList) {
      if (this.containersByTag.has(container.ami_tag)) {
        const currentContainer = this.containersByTag.get(container.ami_tag);
        currentContainer.push(container);
        this.containersByTag.set(container.ami_tag, currentContainer);
      } else {
        this.containersByTag.set(container.ami_tag, [container]);
      }
    }
    const amiTags: string[] = [];
    for (const [amiTag, containerByTag] of this.containersByTag.entries()){
      const dataSource = new MatTableDataSource<GroupProductionDeletionContainer>();
      dataSource.data = containerByTag;
      dataSource.filterPredicate = this.filterDeletionTablePredicate;
      const selection =  new SelectionModel<GroupProductionDeletionContainer>(true, []);
      this.containersByTagTables.push({amiTag, dataSource, selection});
      amiTags.push(amiTag);
    }
    this.amiTagsDescription = new Map<string, AMITag>();
    this.amiTagService.getAMITagDetails(amiTags.join(',')).subscribe(amiTagsDetails => {
      this.amiTagsDescription = amiTagsDetails;
    });
    this.route.fragment.subscribe(f => {
          this.viewportScroller.scrollToAnchor(f);
      });
    this.applyFilter(this.mainFilter);
    this.recountFiltered();
    this.recountSelected();


  }

  filterDeletionTablePredicate(data: GroupProductionDeletionContainer, filter: string): boolean{
    const searchTerms = JSON.parse(filter);
    return (!searchTerms[0] || searchTerms[0] === '' || data.container.toLowerCase().includes(searchTerms[0].trim().toLowerCase())) &&
      (!searchTerms[1] || searchTerms[1] === '' || searchTerms[1] === data.is_expired);
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
    this.sendClicked = true;
    const selectedContainers: GroupProductionDeletionContainer[] = [];
    for (const GPAMIContainers of this.containersByTagTables) {
       GPAMIContainers.dataSource.data.forEach(row => {
          if (GPAMIContainers.selection.isSelected(row)){
            selectedContainers.push(row);
          }
        });
      }
    this.gpDeletionContainerService.askExtension({message: this.extendMessage,
      containers: selectedContainers, number_of_extensions: this.extendNumbers}).subscribe( _ => {
        this.gpDeletionContainerService.getGPDeletionPerOutput(this.outputType, this.dataType).subscribe(newGPList =>{
            this.gpList = newGPList;
            this.fillTablesByTag();
            this.sendClicked = false;
        });
    });
  }
  applyFilter(filterValue): void{
    for (const GPAMIContainers of this.containersByTagTables) {
      GPAMIContainers.dataSource.filter = JSON.stringify([filterValue, this.expiredFilter]);

    }
  }
  recountFiltered(): void{
    this.totalFiltered = 0;
    this.totalFilteredSize = 0;
    this.totalFilteredExpired = 0;
    this.totalFilteredExpiredSize = 0;
    for (const GPAMIContainers of this.containersByTagTables) {
      GPAMIContainers.dataSource.data.forEach(row => {
        this.totalFiltered += 1;
        this.totalFilteredSize += Number(row.size);
        if (row.is_expired === 'expired'){
          this.totalFilteredExpired += 1;
          this.totalFilteredExpiredSize += Number(row.size);
        }
      });
    }
  }

  applyFilterEvent($event: KeyboardEvent): void {
    const filterValue = (event.target as HTMLInputElement).value;
    if (filterValue !== ''){
      this.router.navigate(['.'],
        { queryParams: {main_filter: filterValue}, queryParamsHandling: 'merge' , relativeTo: this.route });
    } else {
      this.router.navigate(['.'],
        { queryParams: {main_filter: null}, queryParamsHandling: 'merge' , relativeTo: this.route });
    }
    this.applyFilter(filterValue);
  }

  changeExpiredFilter(): void {
    if (this.expiredFilter !== ''){
      this.router.navigate(['.'],
        { queryParams: {expired: this.expiredFilter}, queryParamsHandling: 'merge' , relativeTo: this.route });
    } else {
      this.router.navigate(['.'],
        { queryParams: {expired: null}, queryParamsHandling: 'merge' , relativeTo: this.route });
    }
    this.applyFilter(this.mainFilter);


  }
}
