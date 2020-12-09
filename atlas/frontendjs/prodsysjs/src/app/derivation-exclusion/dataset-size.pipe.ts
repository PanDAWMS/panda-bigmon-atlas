
import { Pipe, PipeTransform } from '@angular/core';

type unit = 'bytes' | 'KB' | 'MB' | 'GB' | 'TB' | 'PB';
type unitPrecisionMap = {
  [u in unit]: number;
};

const defaultPrecisionMap: unitPrecisionMap = {
  bytes: 0,
  KB: 0,
  MB: 1,
  GB: 1,
  TB: 2,
  PB: 2
};


@Pipe({ name: 'datasetSize' })
export class DatasetSizePipe implements PipeTransform {
  private readonly units: unit[] = ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];

  transform(bytes: number = 0, precision: number | unitPrecisionMap = defaultPrecisionMap): string {
    if (isNaN(parseFloat(String(bytes))) || !isFinite(bytes)) { return '?'; }

    let unitIndex = 0;

    while (bytes >= 1000) {
      bytes /= 1000;
      unitIndex++;
    }

    const currentUnit = this.units[unitIndex];

    if (typeof precision === 'number') {
      return `${bytes.toFixed(+precision)} ${currentUnit}`;
    }
    return `${bytes.toFixed(precision[currentUnit])} ${currentUnit}`;
  }
}
