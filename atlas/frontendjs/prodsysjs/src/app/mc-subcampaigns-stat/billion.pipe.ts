import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'billion',
  standalone: true
})
export class BillionPipe implements PipeTransform {

  transform(value: number): string {
    return (value / 1e9).toFixed(3) + 'B';
  }

}
