import { Component } from '@angular/core';
import {DerivationPhysicContainerService} from "./derivation-physic-container.service";
import {ActivatedRoute} from "@angular/router";
import {switchMap} from "rxjs/operators";
import {AsyncPipe, JsonPipe} from "@angular/common";

@Component({
  selector: 'app-derivation-physic-container',
  standalone: true,
  imports: [
    AsyncPipe,
    JsonPipe
  ],
  templateUrl: './derivation-physic-container.component.html',
  styleUrl: './derivation-physic-container.component.css'
})
export class DerivationPhysicContainerComponent {

  constructor(private derivationPhysicContainerService: DerivationPhysicContainerService, private route: ActivatedRoute) { }

  public preparedContainers$ = this.route.paramMap.pipe(switchMap((params) => {
    return this.derivationPhysicContainerService.getPhysicContainers(params.get('requestID'), '');
  }
  ));
}
