import { CommonModule, NgForOf, AsyncPipe } from '@angular/common';
import {Component, Input, OnDestroy, OnInit, forwardRef, EventEmitter, Output} from '@angular/core';
import { ControlValueAccessor, FormControl, NG_VALUE_ACCESSOR, ReactiveFormsModule } from '@angular/forms';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { NgxMatSelectSearchModule } from 'ngx-mat-select-search';
import { BehaviorSubject, ReplaySubject, Subject } from 'rxjs';
import { take, takeUntil } from 'rxjs/operators';
import {MatFormField, MatLabel} from "@angular/material/form-field";

@Component({
  selector: 'app-select-with-search',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    MatSelectModule,
    MatOptionModule,
    NgxMatSelectSearchModule,
    NgForOf,
    AsyncPipe,
    MatFormField,
    MatLabel

  ],
  template: `
    <mat-form-field appearance="fill">
  <mat-label>{{label}}</mat-label>
    <mat-select [formControl]="selectControl" [multiple]="multiple" (selectionChange)="onSelectionChange($event)">
      <mat-option>
        <ngx-mat-select-search
          [showToggleAllCheckbox]="multiple"
          (toggleAll)="toggleSelectAll($event)"
          [placeholderLabel]="searchPlaceholder"
          [formControl]="searchFilterControl"
          [toggleAllCheckboxTooltipMessage]="'Select All / Unselect All'"
          [toggleAllCheckboxChecked]="isAllSelected()"
          [noEntriesFoundLabel]="'Nothing found'"
          [toggleAllCheckboxTooltipPosition]="'above'">
        </ngx-mat-select-search>
      </mat-option>
      <mat-option *ngFor="let option of filteredOptions$ | async" [value]="option">
        {{displayOption(option)}}
      </mat-option>
    </mat-select>
    </mat-form-field>
  `,
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => SelectWithSearchComponent),
      multi: true
    }
  ]
})
export class SelectWithSearchComponent implements OnInit, OnDestroy, ControlValueAccessor {
  @Input() options: any[] = [];
  @Input() multiple = false;
  @Input() searchPlaceholder = 'Search';
  @Input() label = '';
  @Output() selectionChange = new EventEmitter<any>();

  selectControl = new FormControl();
  searchFilterControl = new FormControl('');
  filteredOptions$: BehaviorSubject<any[]> = new BehaviorSubject<any[]>([]);
  private destroy$ = new Subject<void>();
  @Input() displayFn: (option: any) => string = (option) => option;

  private onChangeFn: any = () => {};
  private onTouchedFn: any = () => {};

  ngOnInit() {
    // Initialize filtered options
    this.filteredOptions$.next(this.options.slice());

    // Set up search filter
    this.searchFilterControl.valueChanges
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        this.filterOptions();
      });

    // Forward selection changes to ControlValueAccessor
    this.selectControl.valueChanges
      .pipe(takeUntil(this.destroy$))
      .subscribe(value => {
        this.onChangeFn(value);
      });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

  // ControlValueAccessor methods
  writeValue(value: any): void {
    this.selectControl.setValue(value, { emitEvent: false });
  }

  registerOnChange(fn: any): void {
    this.onChangeFn = fn;
  }

  registerOnTouched(fn: any): void {
    this.onTouchedFn = fn;
  }

  setDisabledState(isDisabled: boolean): void {
    isDisabled ? this.selectControl.disable() : this.selectControl.enable();
  }

  onSelectionChange(event: any): void {
    this.onTouchedFn();
  }

  toggleSelectAll(selectAllValue: boolean): void {
    this.filteredOptions$.pipe(take(1), takeUntil(this.destroy$))
      .subscribe(filteredOptions => {
        if (selectAllValue) {
          // If multiple is true, select all filtered options
          if (this.multiple) {
            const currentSelection = this.selectControl.value || [];
            const optionsToAdd = filteredOptions.filter(
              option => !currentSelection.includes(option)
            );
            this.selectControl.setValue([...currentSelection, ...optionsToAdd]);
          }
        } else {
          // If multiple is true, deselect all filtered options
          if (this.multiple) {
            const currentSelection = this.selectControl.value || [];
            const remainingSelection = currentSelection.filter(
              selected => !filteredOptions.includes(selected)
            );
            this.selectControl.setValue(remainingSelection);
          }
        }
      });
  }

  isAllSelected(): boolean {
    if (!this.multiple) { return false; }

    const selectedValues = this.selectControl.value || [];
    const filteredOptions = this.filteredOptions$.getValue();

    // Check if all filtered options are in the current selection
    return filteredOptions.length > 0 &&
           filteredOptions.every(option => selectedValues.includes(option));
  }

  filterOptions(): void {
    if (!this.options) {
      return;
    }

    // Get the search keyword
    let search = this.searchFilterControl.value;
    if (!search) {
      this.filteredOptions$.next(this.options.slice());
      return;
    }

    search = search.toLowerCase();

    // Filter the options
    this.filteredOptions$.next(
      this.options.filter(option =>
        this.displayOption(option).toLowerCase().includes(search)
      )
    );
  }

  displayOption(option: any): string {
    return this.displayFn(option);
  }
}
