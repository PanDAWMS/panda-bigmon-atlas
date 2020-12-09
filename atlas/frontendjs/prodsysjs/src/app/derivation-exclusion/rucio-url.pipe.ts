import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'rucioURL'
})

export class RucioURLPipe implements PipeTransform {

  transform(dataset: string, ...args: unknown[]): string {
    let scope = '';
    let name = '';
    if ( dataset.indexOf(':') > -1 ){
      scope = dataset.split(':')[0];
      name = dataset.split(':')[1];
    } else {
      scope = dataset.split('.')[0];
      name = dataset;
    }

    return `https://rucio-ui.cern.ch/did?scope=${scope}&name=${name}`;
  }

}
