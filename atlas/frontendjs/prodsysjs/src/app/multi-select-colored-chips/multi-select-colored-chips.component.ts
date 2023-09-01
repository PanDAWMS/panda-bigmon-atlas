import {
  AfterViewInit,
  Component,
  Input,
  OnInit,
  ViewChild,
} from '@angular/core';
import { ControlValueAccessor, NG_VALUE_ACCESSOR } from '@angular/forms';
import { MatChip, MatChipList } from '@angular/material/chips';

@Component({
  selector: 'app-chips-multi-select',
  templateUrl: './multi-select-colored-chips.component.html',
  styleUrls: ['./multi-select-colored-chips.component.css', '../production-request/production-request.component.css'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: ChipsMultiSelectColoredComponent,
      multi: true,
    },
  ],
})

export class ChipsMultiSelectColoredComponent
  implements OnInit, AfterViewInit, ControlValueAccessor {
  @ViewChild(MatChipList)
  chipList!: MatChipList;

  @Input() options: string[] = [];
  @Input() optionStyle: string;
  value: string[] = [];

  onChange!: (value: string[]) => void;
  onTouch: any;

  disabled = false;

  constructor() {}

  writeValue(value: string[]): void {
    // When form value set when chips list initialized
    if (this.chipList && value) {
      this.selectChips(value);
    } else if (value) {
      // When chips not initialized
      this.value = value;
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

  ngOnInit(): void {}

  ngAfterViewInit(): void {
    this.selectChips(this.value);

    this.chipList.chipSelectionChanges
      .subscribe((chip) => {
        if (chip.selected) {
          this.value = [...this.value, chip.source.value];
        } else {
          this.value = this.value.filter((o) => o !== chip.source.value);
        }

        this.propagateChange(this.value);
      });
  }

  propagateChange(value: string[]): void {
    if (this.onChange) {
      this.onChange(value);
    }
  }

  selectChips(value: string[]): void {
    this.chipList.chips.forEach((chip) => chip.deselect());

    const chipsToSelect = this.chipList.chips.filter((c) =>
      value.includes(c.value)
    );

    chipsToSelect.forEach((chip) => chip.select());
  }

  toggleSelection(chip: MatChip): void {
    if (!this.disabled){
      chip.toggleSelected();
    }
  }
}
