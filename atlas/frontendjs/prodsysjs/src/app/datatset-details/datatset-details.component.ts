import {Component, computed, inject, input} from '@angular/core';
import {RucioDIDComponent} from "../production-request/rucio-did/rucio-did.component";
import {DSIDInfoService} from "../dsid-info/dsid-info.service";
import {DataCarouselService} from "../DataCarousel/data-carousel.service";
import {toObservable} from "@angular/core/rxjs-interop";
import {RucioContainerComponent} from "../production-request/rucio-container/rucio-container.component";

@Component({
  selector: 'app-datatset-details',
  imports: [
    RucioDIDComponent,
    RucioContainerComponent
  ],
  templateUrl: './datatset-details.component.html',
  styleUrl: './datatset-details.component.css'
})
export class DatatsetDetailsComponent {
    did= input<string>('');
    did$ = toObservable(this.did);
    dsidService = inject(DataCarouselService);
    didType =  this.dsidService.didTypeResource.value;
    constructor() {
        this.did$.subscribe(did => {
            if (did) {
                this.dsidService.containerOrDataset.set(did);
            }
        });
    }

}
