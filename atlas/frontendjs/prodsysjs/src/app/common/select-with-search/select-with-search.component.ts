import { CommonModule, NgForOf, AsyncPipe } from '@angular/common';
import {Component, Input, OnDestroy, OnInit, forwardRef, EventEmitter, Output, inject} from '@angular/core';
import { ControlValueAccessor, FormControl, NG_VALUE_ACCESSOR, ReactiveFormsModule } from '@angular/forms';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { NgxMatSelectSearchModule } from 'ngx-mat-select-search';
import { BehaviorSubject, ReplaySubject, Subject } from 'rxjs';
import { take, takeUntil } from 'rxjs/operators';
import {MatFormField, MatLabel} from "@angular/material/form-field";
import {Params, Router} from "@angular/router";

export class FilterBase {
  public allValues: string[] = [];
  public selectedValues: FormControl<string[]> = new FormControl([]);
  public label: string;
  public urlUpdateParamName: string;


  constructor(label: string, initialValues: string[] = [],  urlUpdateParamName: string = '') {
    this.label = label;
    this.urlUpdateParamName = urlUpdateParamName;
    this.allValues = initialValues;
    this.selectedValues.setValue([]);
  }

  updateValues(values: string[]): void {
    this.allValues = values;
    this.selectedValues.setValue([]);
  }

  takeValuesFromParams(params: Params): void {
      const valuesParam = params[this.urlUpdateParamName];
      if (valuesParam && Array.isArray(valuesParam)) {
        this.selectedValues.setValue(valuesParam);
      } else if (valuesParam && typeof valuesParam === 'string') {
        this.selectedValues.setValue([valuesParam]);
      } else {
        this.selectedValues.setValue([]);
      }

  }
}

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
  <mat-label>{{filterBase.label}}</mat-label>
    <mat-select [panelWidth]="''" [formControl]="selectControl" [multiple]="multiple" (selectionChange)="onSelectionChange($event)">
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
  @Input() multiple = false;
  @Input() searchPlaceholder = 'Search';
  @Input() filterBase: FilterBase;
  @Output() selectionChange = new EventEmitter<any>();
  selectControl = new FormControl();
  searchFilterControl = new FormControl('');
  filteredOptions$: BehaviorSubject<any[]> = new BehaviorSubject<any[]>([]);
  private destroy$ = new Subject<void>();
  @Input() displayFn: (option: any) => string = (option) => option;
  private router = inject(Router);
  private onChangeFn: any = () => {};
  private onTouchedFn: any = () => {};

  ngOnInit() {
    // Initialize filtered options
    this.filteredOptions$.next(this.filterBase.allValues.slice());
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
        if (this.filterBase.urlUpdateParamName !== '') {
             const newQueryParams = {queryParams: {}};
             if (
                !value ||
                value.length === 0 ||
                (this.filterBase.allValues && JSON.stringify(value.sort()) === JSON.stringify(this.filterBase.allValues.sort()))
              ) {
                // Remove parameter if ALL or none selected
                newQueryParams.queryParams[this.filterBase.urlUpdateParamName] = null;
              } else {
                newQueryParams.queryParams[this.filterBase.urlUpdateParamName] = value;
              }
             this.router.navigate([], {queryParams: newQueryParams.queryParams, queryParamsHandling: 'merge'});
        }
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
    if (!this.filterBase.allValues) {
      return;
    }

    // Get the search keyword
    let search = this.searchFilterControl.value;
    if (!search) {
      this.filteredOptions$.next(this.filterBase.allValues.slice());
      return;
    }

    search = search.toLowerCase();

    // Filter the options
    this.filteredOptions$.next(
      this.filterBase.allValues.filter(option =>
        this.displayOption(option).toLowerCase().includes(search)
      )
    );
  }

  displayOption(option: any): string {
    return this.displayFn(option);
  }
}
