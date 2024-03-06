import {ChangeDetectorRef, Component, Input, OnInit} from '@angular/core';
import { ControlValueAccessor, NG_VALUE_ACCESSOR } from '@angular/forms';
import {SelectionModel} from "@angular/cdk/collections";

@Component({
  selector: 'app-task-stats',
  templateUrl: './task-stats.component.html',
  styleUrls: ['./task-stats.component.css', '../production-request.component.css'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: TaskStatsComponent,
      multi: true,
    },
  ],
})
export class TaskStatsComponent implements OnInit, ControlValueAccessor  {

  @Input() taskStatus: {[status: string]: number};
 // @Input() taskStatusFiltered: {[status: string]: number};
  @Input() collectionStatus?: {[status: string]: string[]};
  @Input() statusOrder?: string[];
  @Input() orientation: 'horizontal' | 'vertical' = 'horizontal';

  value: string[] = [];
  selectedStatus: SelectionModel<string> = new SelectionModel(true, []);
  collectionKeys: Map<string, number> = new Map<string, number>();
  taskStatusOrdered: Map<string, number> = new Map<string, number>();
  collectionKeysSelect: SelectionModel<string> = new SelectionModel(true, []);
  onChange!: (value: string[]) => void;
  onTouch: any;

  disabled = false;

  constructor() { }

  writeValue(value: string[]): void {
    if (this.selectedStatus && value) {
      this.selectedStatus.select(...value);
    } else if (value) {
      this.value = value;
    }
  }

  ngOnInit(): void {
    this.selectedStatus = new SelectionModel<string>(true, Object.keys(this.taskStatus));
    if (!this.collectionStatus){
      this.collectionStatus = {total: Object.keys(this.taskStatus)};
    } else {
          this.collectionStatus.total = Object.keys(this.taskStatus);
    }
    if (this.collectionStatus){
      this.collectionKeysSelect = new SelectionModel<string>(true, Object.keys(this.collectionStatus));
      let collectionKeysOrder: string[] = Object.keys(this.collectionStatus);
      if (this.statusOrder){
        collectionKeysOrder = this.statusOrder.filter(key => key in this.collectionStatus);
      }
      for (const name of collectionKeysOrder) {
        const intersectionStatus = this.collectionStatus[name].filter(val => this.selectedStatus.selected.includes(val));
        if (intersectionStatus.length > 0){
          const statusCount = intersectionStatus.reduce<number>((acc, currentStatus) => {
            return acc + this.taskStatus[currentStatus];
          }, 0);
          this.collectionKeys.set(name, statusCount);
        }
      }
    }
    if (this.statusOrder){
      this.statusOrder.forEach(status => {
        if (status in this.taskStatus){
          this.taskStatusOrdered.set(status, this.taskStatus[status]);
        }
      });
    }

  }
  registerOnChange(fn: any): void {
    this.onChange = fn;
  }

  registerOnTouched(fn: any): void {
    this.onTouch = fn;
  }

  setDisabledState?(isDisabled: boolean): void {
    this.disabled = isDisabled;
  }


  selectStatus(key: string ): void {
    this.selectedStatus.toggle(key);
    this.value = this.selectedStatus.selected;
    this.propagateChange(this.value);
    this.selectCollection();
  }
  toggleCollection(key: string): void{
    this.selectedStatus.clear();
    this.selectedStatus.select(...this.collectionStatus[key]);
    this.collectionKeysSelect.clear();
    this.collectionKeysSelect.select(...[key]);
    this.value = this.selectedStatus.selected;
    this.propagateChange(this.value);
  }

  selectSingleStatus(key: string ): void {
    this.selectedStatus.clear();
    this.selectedStatus.select(...[key]);
    this.value = this.selectedStatus.selected;
    this.propagateChange(this.value);
    this.selectCollection();
  }
  selectCollection(): void {
    this.collectionKeysSelect.clear();
    for (const name of Object.keys(this.collectionKeys)){
      const intersectionStatus = this.collectionStatus[name].filter(val => this.selectedStatus.selected.includes(val));
      if (intersectionStatus.length === this.collectionStatus[name].length){
        this.collectionKeysSelect.select(...[name]);
      }
    }
  }
  propagateChange(value: string[]): void {
    if (this.onChange) {
      this.onChange(value);
    }
  }

  asIsOrder(): number {
    return 0;
  }
}
